#!/usr/bin/env python3
"""
DRY-RUN test script for DJ City track import pipeline.
Connects to Dropbox + Database (read-only) + Deezer API, and for each audio file shows:
  - Parsed BPM, type, cleaned title, artist
  - Database field the file would go into
  - Whether it would CREATE a new track or UPDATE an existing one
  - Matched reference artists from the DB
  - Deezer metadata: ISRC, BPM, album, label, genre, cover URL
  - Skip filtering

Nothing is written to the database. This is 100% read-only.

Usage:
    python3 test_parsing.py                              # Scan Dropbox root /ID 2026
    python3 test_parsing.py "/ID 2026/DJ CITY/2022"      # Scan specific folder
    python3 test_parsing.py --local file1.mp3 file2.mp3   # Test filenames only (no Dropbox/DB)
    python3 test_parsing.py --no-deezer "/ID 2026/..."    # Skip Deezer API calls
"""

import re
import os
import sys
import json
import time
import urllib.parse

# ─── Load .env ───────────────────────────────────────────────────────────────────

ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value

try:
    import requests
except ImportError:
    print("Error: 'requests' package required. Run: pip install requests")
    sys.exit(1)

try:
    import psycopg2
    import psycopg2.extras
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False


# ─── Colors ──────────────────────────────────────────────────────────────────────

class C:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    END = '\033[0m'


# ═══════════════════════════════════════════════════════════════════════════════════
# DROPBOX
# ═══════════════════════════════════════════════════════════════════════════════════

DROPBOX_ACCESS_TOKEN = os.environ.get('DROPBOX_ACCESS_TOKEN', '')
DROPBOX_REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', '')
DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')
DROPBOX_APP_SECRET = os.environ.get('DROPBOX_APP_SECRET', '')
DROPBOX_TEAM_MEMBER_ID = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '')
_current_token = DROPBOX_ACCESS_TOKEN


def refresh_dropbox_token():
    global _current_token
    if not (DROPBOX_REFRESH_TOKEN and DROPBOX_APP_KEY and DROPBOX_APP_SECRET):
        return _current_token
    try:
        resp = requests.post(
            'https://api.dropbox.com/oauth2/token',
            data={'grant_type': 'refresh_token', 'refresh_token': DROPBOX_REFRESH_TOKEN},
            auth=(DROPBOX_APP_KEY, DROPBOX_APP_SECRET), timeout=15)
        if resp.status_code == 200:
            _current_token = resp.json().get('access_token', '')
            return _current_token
    except Exception as e:
        print(f"  Token refresh error: {e}")
    return _current_token


def get_token():
    global _current_token
    refreshed = refresh_dropbox_token()
    if refreshed:
        _current_token = refreshed
    return _current_token


def list_dropbox_files(folder_path=''):
    token = get_token()
    if not token:
        print(f"{C.RED}No Dropbox token. Check .env{C.END}")
        sys.exit(1)

    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    if DROPBOX_TEAM_MEMBER_ID:
        headers['Dropbox-API-Select-User'] = DROPBOX_TEAM_MEMBER_ID

    namespace_id = ''
    if DROPBOX_TEAM_MEMBER_ID:
        try:
            acct_resp = requests.post(
                'https://api.dropboxapi.com/2/users/get_current_account',
                headers={'Authorization': f'Bearer {token}',
                         'Dropbox-API-Select-User': DROPBOX_TEAM_MEMBER_ID}, timeout=15)
            if acct_resp.status_code == 200:
                namespace_id = acct_resp.json().get('root_info', {}).get('root_namespace_id', '')
        except:
            pass
    if namespace_id:
        headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})

    all_files = []
    has_more = True
    cursor = None
    retried = False

    while has_more:
        try:
            if cursor:
                resp = requests.post('https://api.dropboxapi.com/2/files/list_folder/continue',
                                     headers=headers, json={'cursor': cursor}, timeout=30)
            else:
                resp = requests.post('https://api.dropboxapi.com/2/files/list_folder',
                                     headers=headers,
                                     json={'path': folder_path, 'recursive': True,
                                           'include_media_info': False, 'include_deleted': False,
                                           'limit': 2000}, timeout=30)

            if resp.status_code == 401 and not retried:
                retried = True
                token = refresh_dropbox_token()
                headers['Authorization'] = f'Bearer {token}'
                cursor = None
                continue

            if resp.status_code != 200:
                print(f"{C.RED}Dropbox error ({resp.status_code}): {resp.text[:200]}{C.END}")
                sys.exit(1)

            result = resp.json()
            for entry in result.get('entries', []):
                if entry.get('.tag') == 'file':
                    name_lower = entry.get('name', '').lower()
                    if name_lower.endswith('.mp3') or name_lower.endswith('.wav'):
                        all_files.append({
                            'name': entry['name'],
                            'path': entry.get('path_display', ''),
                            'size': entry.get('size', 0),
                        })
            has_more = result.get('has_more', False)
            cursor = result.get('cursor')
            print(f"\r  Scanning... {len(all_files)} audio files", end='', flush=True)
        except requests.exceptions.Timeout:
            time.sleep(2)
            continue
        except Exception as e:
            print(f"\n  Error: {e}")
            break
    print()
    return all_files


# ═══════════════════════════════════════════════════════════════════════════════════
# DATABASE (read-only)
# ═══════════════════════════════════════════════════════════════════════════════════

_db_conn = None
_ref_artists_cache = None
_artists_cache = None


def db_connect():
    global _db_conn
    if _db_conn:
        return _db_conn
    if not DB_AVAILABLE:
        return None
    db_url = os.environ.get('DATABASE_URL', '')
    if not db_url:
        return None
    try:
        _db_conn = psycopg2.connect(db_url)
        _db_conn.set_session(readonly=True, autocommit=True)
        return _db_conn
    except Exception as e:
        print(f"  {C.YELLOW}DB connection failed: {e}{C.END}")
        return None


def db_close():
    global _db_conn
    if _db_conn:
        try:
            _db_conn.close()
        except:
            pass
        _db_conn = None


def load_reference_artists():
    global _ref_artists_cache
    if _ref_artists_cache is not None:
        return _ref_artists_cache
    conn = db_connect()
    if not conn:
        _ref_artists_cache = []
        return _ref_artists_cache
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT id, name FROM "ReferenceArtist" WHERE name IS NOT NULL AND name != \'\'')
            _ref_artists_cache = cur.fetchall()
    except Exception as e:
        print(f"  {C.YELLOW}Could not load reference artists: {e}{C.END}")
        _ref_artists_cache = []
    return _ref_artists_cache


def load_artists():
    global _artists_cache
    if _artists_cache is not None:
        return _artists_cache
    conn = db_connect()
    if not conn:
        _artists_cache = []
        return _artists_cache
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT id, name FROM "Artist" WHERE name IS NOT NULL AND name != \'\'')
            _artists_cache = cur.fetchall()
    except Exception as e:
        print(f"  {C.YELLOW}Could not load artists: {e}{C.END}")
        _artists_cache = []
    return _artists_cache


def find_matching_reference_artists(artist_string):
    all_ref = load_reference_artists()
    if not all_ref or not artist_string:
        return []
    sorted_refs = sorted(all_ref, key=lambda x: len(x[1] or ''), reverse=True)
    search_string = artist_string.lower()
    found = []
    for ref_id, ref_name in sorted_refs:
        if not ref_name or len(ref_name) < 2:
            continue
        try:
            regex = re.compile(rf'\b{re.escape(ref_name.lower())}\b', re.IGNORECASE)
            if regex.search(search_string):
                found.append((ref_id, ref_name))
                search_string = regex.sub(' ' * len(ref_name), search_string)
        except:
            pass
    return found


def find_matching_artist(artist_string):
    all_artists = load_artists()
    if not all_artists or not artist_string:
        return None
    artist_lower = artist_string.lower()
    for aid, aname in all_artists:
        if aname and aname.lower() == artist_lower:
            return (aid, aname)
    for aid, aname in all_artists:
        if aname and artist_lower in aname.lower():
            return (aid, aname)
    return None


def find_existing_track(track_id):
    conn = db_connect()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('''
                SELECT id, "trackId", title, "editTitle", bpm, "originalArtist",
                       "trackFile_filename", "originalTrackMp3_filename",
                       "originalTrackMp3Clean_filename", "originalTrackMp3Dirty_filename",
                       "extendedTrackMp3_filename", "instru_filename", "intro_filename",
                       "acapella_filename", "coverImage_id", "ISRC"
                FROM "Track"
                WHERE "trackId" = %s
                LIMIT 1
            ''', (track_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        return None


# ═══════════════════════════════════════════════════════════════════════════════════
# DEEZER API
# ═══════════════════════════════════════════════════════════════════════════════════

def search_deezer_metadata(artist, title, timeout=10):
    """
    Search Deezer for ISRC, BPM, album, label, genre, release date, and cover art.
    Two API calls: /search then /track/{id} for full details.
    Free, no auth, ~50 req/5s rate limit.
    """
    result = {
        'deezer_id': None, 'isrc': '', 'bpm': None,
        'title': '', 'artist': '', 'album': '', 'label': '',
        'release_date': '', 'genre': '', 'cover_url': '', 'cover_url_medium': '',
        'duration': None, 'explicit': False, 'deezer_link': '',
    }
    if not artist or not title:
        return result

    clean_artist = re.sub(r'\s*(feat\.?|ft\.?|featuring)\s+.*$', '', artist, flags=re.IGNORECASE).strip()
    clean_title = re.sub(r'\s*\(.*?\)', '', title).strip()
    clean_title = re.sub(r'\s*-\s*$', '', clean_title).strip()

    try:
        search_url = 'https://api.deezer.com/search'
        params = {'q': f'artist:"{clean_artist}" track:"{clean_title}"', 'limit': 5}
        resp = requests.get(search_url, params=params, timeout=timeout)
        if resp.status_code != 200:
            return result
        tracks = resp.json().get('data', [])

        if not tracks:
            params = {'q': f'{clean_artist} {clean_title}', 'limit': 5}
            resp = requests.get(search_url, params=params, timeout=timeout)
            if resp.status_code == 200:
                tracks = resp.json().get('data', [])
        if not tracks:
            return result

        track = tracks[0]
        deezer_id = track.get('id')
        result['deezer_id'] = deezer_id
        result['title'] = track.get('title', '')
        result['artist'] = track.get('artist', {}).get('name', '')
        result['album'] = track.get('album', {}).get('title', '')
        result['cover_url'] = track.get('album', {}).get('cover_xl', '')
        result['cover_url_medium'] = track.get('album', {}).get('cover_big', '')
        result['duration'] = track.get('duration')
        result['explicit'] = track.get('explicit_lyrics', False)
        result['deezer_link'] = track.get('link', '')

        if deezer_id:
            detail_resp = requests.get(f'https://api.deezer.com/track/{deezer_id}', timeout=timeout)
            if detail_resp.status_code == 200:
                detail = detail_resp.json()
                result['isrc'] = detail.get('isrc', '')
                result['bpm'] = detail.get('bpm') if detail.get('bpm') and detail.get('bpm') > 0 else None
                album_data = detail.get('album', {})
                if album_data:
                    result['label'] = album_data.get('label', '')
                    result['release_date'] = album_data.get('release_date', '')
                    if not result['cover_url']:
                        result['cover_url'] = album_data.get('cover_xl', '')
                    genres = album_data.get('genres', {}).get('data', [])
                    if genres:
                        result['genre'] = genres[0].get('name', '')
    except Exception as e:
        pass  # Silently fail for test script

    return result


# ═══════════════════════════════════════════════════════════════════════════════════
# PARSING (same as app.py)
# ═══════════════════════════════════════════════════════════════════════════════════

TYPE_TO_FILE_FIELD_MAP = {
    'Main': 'trackFile', 'Extended': 'extendedTrackMp3',
    'Original': 'originalTrackMp3', 'Original Clean': 'originalTrackMp3Clean',
    'Original Dirty': 'originalTrackMp3Dirty', 'Intro': 'intro',
    'Instrumental': 'instru', 'Acapella': 'acapella',
    'Extended Clean': 'extendedTrackMp3Clean', 'Extended Dirty': 'extendedTrackMp3Dirty',
}

SKIP_KEYWORDS = [
    'rework', 're-work', 'boot', 'bootleg', 'mashup', 'mash-up', 'mash up',
    'riddim', 'ridim', 'redrum', 're-drum', 'transition',
    'hype', 'throwback hype', 'wordplay', 'tonalplay', 'tonal play', 'toneplay',
    'beat intro', 'segway', 'segue', 'edit',
    'blend', 'anthem', 'club', 'halloween', 'christmas', 'easter',
    'countdown', 'private', 'party break',
    'sample', 'chill mix', 'kidcutup', 'kid cut up',
    'bounce back', 'chorus in', 'orchestral',
    'da phonk', 'daphonk', 'epice intro', 'epic intro',
    'remix',
]


def detect_track_type_from_title(title):
    if not title:
        return None
    tl = title.lower()
    if re.search(r'\(\s*inst(?:rumental)?\s*\)', tl):
        return 'Instrumental'
    if re.search(r'\(\s*(?:[\w\s]*\s+)?intro(?:\s*-\s*(?:clean|dirty))?\s*\)', tl):
        return 'Intro'
    if re.search(r'intro\s*-\s*(?:clean|dirty)', tl):
        return 'Intro'
    if re.search(r'\(\s*clean\s*\)', tl):
        return 'Original Clean'
    if re.search(r'\(\s*dirty\s*\)', tl):
        return 'Original Dirty'
    if 'instrumental' in tl or '(inst)' in tl:
        return 'Instrumental'
    elif 'acapella' in tl or 'a capella' in tl or 'acappella' in tl:
        return 'Acapella'
    elif 'extended' in tl:
        return 'Extended'
    return None


def extract_bpm_from_filename(filename):
    name = os.path.splitext(filename)[0]
    m = re.search(r'(\d{2,3})\s*$', name)
    if m:
        bpm = int(m.group(1))
        if 60 <= bpm <= 200:
            return bpm
    return None


def clean_detected_type_from_title(title, detected_type=None):
    if not title:
        return title
    c = title
    c = re.sub(r'\.(mp3|wav|flac|aac|ogg|m4a)$', '', c, flags=re.IGNORECASE)
    c = re.sub(r'\s+\d{2,3}\s*$', '', c)
    c = re.sub(r'\s*\(\s*(?:clean|dirty|inst(?:rumental)?)\s*\)', '', c, flags=re.IGNORECASE)
    c = re.sub(r'\s*\(\s*(?:[\w\s]*\s+)?intro(?:\s*-\s*(?:clean|dirty))?\s*\)', '', c, flags=re.IGNORECASE)
    if ' - ' in c:
        parts = c.split(' - ', 1)
        if len(parts) > 1:
            c = parts[1]
    edit_kw = ['remix', 'edit', 'intro', 'outro', 'transition', 'hype', 'club',
               'bootleg', 'mashup', 'blend', 'rework', 'redrum', 'flip',
               'version', 'mix', 'dub', 'vip', 'break intro', 'slam']
    if ' - ' in c:
        parts = c.rsplit(' - ', 1)
        if len(parts) == 2:
            after = parts[1].lower()
            for kw in edit_kw:
                if kw in after:
                    c = parts[0]
                    break
    return re.sub(r'\s+', ' ', c).strip()


def should_skip_track(title):
    """Check if a track should be skipped (keyword filter only). Returns (should_skip, reason)."""
    if not title:
        return False, None
    tl = title.lower()
    for kw in SKIP_KEYWORDS:
        if kw.lower() in tl:
            return True, f"Contains '{kw}'"
    return False, None


def get_db_field(track_type):
    if not track_type:
        return 'trackFile'
    f = TYPE_TO_FILE_FIELD_MAP.get(track_type)
    if not f:
        for k, v in TYPE_TO_FILE_FIELD_MAP.items():
            if k.lower() == track_type.lower():
                f = v
                break
    return f or 'trackFile'


def extract_artist(filename):
    name = os.path.splitext(filename)[0]
    name = re.sub(r'\s+\d{2,3}\s*$', '', name)
    if ' - ' in name:
        return name.split(' - ', 1)[0].strip()
    return 'Unknown'


def build_track_id(clean_title, artist):
    combined = f"{artist} - {clean_title}" if artist != 'Unknown' else clean_title
    tid = combined.replace('-', ' ').replace('_', ' ')
    tid = re.sub(r'\s+', ' ', tid).strip()
    tid = tid.replace(' ', '_')
    tid = re.sub(r'_+', '_', tid)
    return tid


def format_size(size_bytes):
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.0f}KB"
    return f"{size_bytes / (1024 * 1024):.1f}MB"


# ═══════════════════════════════════════════════════════════════════════════════════
# ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════════

def analyze_track(filename, path='', size=0, use_db=False, use_deezer=False):
    bpm = extract_bpm_from_filename(filename)
    track_type = detect_track_type_from_title(filename)
    clean_title = clean_detected_type_from_title(filename)
    artist = extract_artist(filename)
    db_field = get_db_field(track_type)

    # Keyword-based skip (remix, edit, transition, etc.)
    skip, skip_reason = should_skip_track(filename)

    info = {
        'filename': filename, 'path': path, 'size': size,
        'artist': artist, 'clean_title': clean_title, 'bpm': bpm,
        'track_type': track_type, 'db_field': db_field,
        'skip': skip, 'skip_reason': skip_reason,
        # DB lookups
        'action': None, 'existing_track': None, 'track_id': None,
        'ref_artists': [], 'artist_match': None, 'field_occupied': False,
        # Deezer metadata
        'deezer': None,
    }

    if skip:
        return info

    # Build trackId
    track_id = build_track_id(clean_title, artist)
    info['track_id'] = track_id

    # DB lookups
    if use_db:
        existing = find_existing_track(track_id)
        info['existing_track'] = existing
        if existing:
            info['action'] = 'UPDATE'
            field_col = f"{db_field}_filename"
            if existing.get(field_col):
                info['field_occupied'] = True
        else:
            info['action'] = 'CREATE'
        info['ref_artists'] = find_matching_reference_artists(artist)
        info['artist_match'] = find_matching_artist(artist)

    # Deezer API lookup
    if use_deezer:
        dz = search_deezer_metadata(artist, clean_title)
        if dz.get('deezer_id'):
            info['deezer'] = dz
            # Deezer can fill missing BPM
            if not info['bpm'] and dz.get('bpm'):
                info['bpm'] = dz['bpm']

    return info


def print_result(info, index=None):
    prefix = f"  {C.DIM}#{index:<4d}{C.END} " if index is not None else "  "

    if info['skip']:
        print(f"{prefix}{C.RED}SKIP{C.END}  {C.DIM}{info['filename']}{C.END}")
        print(f"              {C.RED}{info['skip_reason']}{C.END}")
        return

    # Action badge
    action = info.get('action')
    if action == 'CREATE':
        badge = f"{C.GREEN}CREATE{C.END}"
    elif action == 'UPDATE':
        badge = f"{C.YELLOW}UPDATE{C.END}"
    else:
        badge = f"{C.DIM}---{C.END}"

    print(f"{prefix}{badge}  {C.BOLD}{info['clean_title']}{C.END}")
    if info['path']:
        print(f"              {C.DIM}{info['path']}  ({format_size(info['size'])}){C.END}")
    print(f"              {C.CYAN}Artist:{C.END}  {info['artist']}")
    print(f"              {C.YELLOW}BPM:{C.END}     {info['bpm'] or '???'}")
    print(f"              {C.BLUE}Type:{C.END}    {info['track_type'] or 'None (Demucs)'}")
    print(f"              {C.HEADER}Field:{C.END}   {info['db_field']}")

    if info.get('track_id'):
        print(f"              {C.DIM}TrackID: {info['track_id']}{C.END}")

    # Deezer results
    dz = info.get('deezer')
    if dz:
        print(f"              {C.BOLD}{C.CYAN}── Deezer ──{C.END}")
        print(f"              {C.CYAN}ISRC:{C.END}    {dz.get('isrc') or '(none)'}")
        if dz.get('bpm'):
            print(f"              {C.CYAN}BPM:{C.END}     {dz['bpm']}")
        print(f"              {C.CYAN}Album:{C.END}   {dz.get('album', '')}")
        print(f"              {C.CYAN}Label:{C.END}   {dz.get('label', '')}")
        print(f"              {C.CYAN}Genre:{C.END}   {dz.get('genre', '')}")
        print(f"              {C.CYAN}Date:{C.END}    {dz.get('release_date', '')}")
        if dz.get('cover_url'):
            print(f"              {C.CYAN}Cover:{C.END}   {dz['cover_url'][:80]}...")
        else:
            print(f"              {C.CYAN}Cover:{C.END}   {C.RED}(no cover){C.END}")
        if dz.get('explicit'):
            print(f"              {C.RED}EXPLICIT{C.END}")
    elif info.get('_deezer_enabled'):
        print(f"              {C.DIM}Deezer: no match found{C.END}")

    # DB results
    if action == 'UPDATE' and info.get('existing_track'):
        ex = info['existing_track']
        occupied = info.get('field_occupied')
        occ_str = f" {C.RED}(ALREADY FILLED - would overwrite){C.END}" if occupied else f" {C.GREEN}(empty - will fill){C.END}"
        has_cover = bool(ex.get('coverImage_id'))
        cover_str = f"{C.RED}HAS COVER -> will be REPLACED{C.END}" if has_cover else f"{C.GREEN}no cover{C.END}"
        has_isrc = bool(ex.get('ISRC'))
        isrc_str = f"ISRC={ex.get('ISRC')}" if has_isrc else "ISRC=(empty)"

        print(f"              {C.YELLOW}Existing DB ID:{C.END} {ex['id']}  title=\"{ex.get('title', '')}\"  bpm={ex.get('bpm', 0)}")
        print(f"              {C.YELLOW}Target field:{C.END} {info['db_field']}{occ_str}")
        print(f"              {C.YELLOW}Cover:{C.END} {cover_str}")
        print(f"              {C.YELLOW}{isrc_str}{C.END}")

    if info.get('ref_artists'):
        names = ', '.join(n for _, n in info['ref_artists'])
        print(f"              {C.GREEN}Ref Artists:{C.END} {names}")
    elif action:
        print(f"              {C.DIM}Ref Artists: (none matched){C.END}")

    if info.get('artist_match'):
        aid, aname = info['artist_match']
        print(f"              {C.GREEN}Artist DB:{C.END} {aname} (id={aid})")
    elif action:
        print(f"              {C.DIM}Artist DB: (no match){C.END}")


def print_summary(results, use_db, use_deezer):
    total = len(results)
    skipped = sum(1 for r in results if r['skip'])
    processed = total - skipped

    types = {}
    bpm_found = bpm_missing = 0
    skip_reasons = {}
    creates = updates = field_conflicts = 0
    ref_matched = ref_unmatched = 0
    artist_matched = artist_unmatched = 0
    dz_matched = dz_isrc = dz_bpm = dz_cover = dz_label = 0
    cover_replace_count = 0

    for r in results:
        if r['skip']:
            skip_reasons[r['skip_reason']] = skip_reasons.get(r['skip_reason'], 0) + 1
            continue
        t = r['track_type'] or 'None (Demucs)'
        types[t] = types.get(t, 0) + 1
        if r['bpm']:
            bpm_found += 1
        else:
            bpm_missing += 1
        if use_db:
            if r['action'] == 'CREATE':
                creates += 1
            elif r['action'] == 'UPDATE':
                updates += 1
            if r.get('field_occupied'):
                field_conflicts += 1
            if r.get('ref_artists'):
                ref_matched += 1
            else:
                ref_unmatched += 1
            if r.get('artist_match'):
                artist_matched += 1
            else:
                artist_unmatched += 1
            # Count cover replacements
            if r.get('existing_track') and r['existing_track'].get('coverImage_id') and r.get('deezer') and r['deezer'].get('cover_url'):
                cover_replace_count += 1
        if use_deezer:
            dz = r.get('deezer')
            if dz:
                dz_matched += 1
                if dz.get('isrc'):
                    dz_isrc += 1
                if dz.get('bpm'):
                    dz_bpm += 1
                if dz.get('cover_url'):
                    dz_cover += 1
                if dz.get('label'):
                    dz_label += 1

    total_size = sum(r['size'] for r in results)
    proc_size = sum(r['size'] for r in results if not r['skip'])

    print(f"\n{C.BOLD}{'=' * 75}{C.END}")
    print(f"{C.BOLD}  SUMMARY{C.END}")
    print(f"{'=' * 75}")
    print(f"  Total files:        {total}  ({format_size(total_size)})")
    print(f"  {C.GREEN}Would process:{C.END}     {processed}  ({format_size(proc_size)})")
    print(f"  {C.RED}Would skip:{C.END}        {skipped}")
    print(f"  {C.YELLOW}BPM extracted:{C.END}     {bpm_found}/{processed}  ({bpm_found * 100 // max(processed, 1)}%)")

    if use_deezer:
        print(f"\n  {C.BOLD}Deezer API results:{C.END}")
        print(f"    {C.CYAN}Matched:{C.END}          {dz_matched}/{processed}  ({dz_matched * 100 // max(processed, 1)}%)")
        print(f"    {C.CYAN}ISRC found:{C.END}       {dz_isrc}/{processed}")
        print(f"    {C.CYAN}BPM found:{C.END}        {dz_bpm}/{processed}")
        print(f"    {C.CYAN}Cover found:{C.END}      {dz_cover}/{processed}")
        print(f"    {C.CYAN}Label found:{C.END}      {dz_label}/{processed}")

    if use_db:
        print(f"\n  {C.BOLD}Database actions:{C.END}")
        print(f"    {C.GREEN}CREATE new track:{C.END}   {creates}")
        print(f"    {C.YELLOW}UPDATE existing:{C.END}    {updates}")
        if field_conflicts:
            print(f"    {C.RED}Field conflicts:{C.END}    {field_conflicts}  (target field already has a file)")
        if cover_replace_count:
            print(f"    {C.RED}Cover REPLACE:{C.END}      {cover_replace_count}  (existing cover will be replaced by Deezer)")

        print(f"\n  {C.BOLD}Artist matching:{C.END}")
        print(f"    {C.GREEN}Ref artists found:{C.END}  {ref_matched}/{processed}")
        print(f"    {C.GREEN}Artist DB found:{C.END}    {artist_matched}/{processed}")
        if ref_unmatched:
            print(f"    {C.RED}Ref artists miss:{C.END}  {ref_unmatched}")
        if artist_unmatched:
            print(f"    {C.RED}Artist DB miss:{C.END}    {artist_unmatched}")

    print(f"\n  {C.BOLD}Type breakdown:{C.END}")
    for t, count in sorted(types.items(), key=lambda x: -x[1]):
        field = get_db_field(t if t != 'None (Demucs)' else None)
        bar = '#' * min(count, 40)
        print(f"    {t:25s} {C.GREEN}{count:4d}{C.END}  {C.DIM}{bar}  -> {field}{C.END}")

    if skip_reasons:
        print(f"\n  {C.BOLD}Skip reasons:{C.END}")
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            bar = '#' * min(count, 40)
            print(f"    {reason:35s} {C.RED}{count:4d}{C.END}  {C.DIM}{bar}{C.END}")

    if use_db:
        unmatched_artists = set()
        for r in results:
            if not r['skip'] and not r.get('ref_artists') and r.get('artist') and r['artist'] != 'Unknown':
                unmatched_artists.add(r['artist'])
        if unmatched_artists:
            print(f"\n  {C.BOLD}Artists with NO reference artist match ({len(unmatched_artists)}):{C.END}")
            for a in sorted(unmatched_artists)[:30]:
                print(f"    {C.RED}-{C.END} {a}")
            if len(unmatched_artists) > 30:
                print(f"    {C.DIM}... and {len(unmatched_artists) - 30} more{C.END}")

    print(f"{'=' * 75}\n")


# ═══════════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════════

def main():
    local_mode = False
    folder_path = '/ID 2026'
    local_files = []
    no_deezer = False

    # Parse args
    args = sys.argv[1:]
    filtered_args = []
    for arg in args:
        if arg == '--no-deezer':
            no_deezer = True
        elif arg in ('--help', '-h'):
            print(__doc__)
            sys.exit(0)
        else:
            filtered_args.append(arg)

    if filtered_args and filtered_args[0] == '--local':
        local_mode = True
        local_files = filtered_args[1:]
    elif filtered_args:
        folder_path = filtered_args[0]

    use_db = False
    use_deezer = not no_deezer

    if local_mode:
        if not local_files:
            print(f"{C.RED}No filenames. Usage: test_parsing.py --local file1.mp3{C.END}")
            sys.exit(1)
        print(f"\n{C.BOLD}Testing {len(local_files)} local filename(s){C.END}")
        if use_deezer:
            print(f"  {C.CYAN}Deezer API: enabled{C.END} (use --no-deezer to skip)")
        print()
        files = [{'name': f, 'path': '', 'size': 0} for f in local_files]
    else:
        # Connect to DB
        print(f"\n{C.BOLD}Connecting to database...{C.END}")
        conn = db_connect()
        if conn:
            use_db = True
            refs = load_reference_artists()
            arts = load_artists()
            print(f"  {C.GREEN}Connected{C.END} - {len(refs)} reference artists, {len(arts)} artists loaded")
        else:
            print(f"  {C.YELLOW}No DB connection (install psycopg2 or check DATABASE_URL). Parsing-only mode.{C.END}")

        if use_deezer:
            print(f"  {C.CYAN}Deezer API: enabled{C.END} (use --no-deezer to skip)")
        else:
            print(f"  {C.DIM}Deezer API: disabled{C.END}")

        # Connect to Dropbox
        print(f"\n{C.BOLD}Scanning Dropbox: {folder_path}{C.END}")
        files = list_dropbox_files(folder_path)
        if not files:
            print(f"{C.YELLOW}No audio files found in '{folder_path}'{C.END}")
            sys.exit(0)
        print(f"  {C.GREEN}{len(files)} audio files found{C.END}\n")

    files.sort(key=lambda f: f['name'].lower())

    # Analyze all
    total = len(files)
    non_skip = 0
    print(f"{C.BOLD}Analyzing...{C.END}")
    results = []
    for i, f in enumerate(files):
        r = analyze_track(f['name'], f.get('path', ''), f.get('size', 0),
                          use_db=use_db, use_deezer=False)  # First pass: no Deezer yet (to count non-skipped)
        if not r['skip']:
            non_skip += 1
        results.append(r)
        if (i + 1) % 200 == 0:
            print(f"\r  {i + 1}/{total} pre-scanned...", end='', flush=True)
    if total >= 200:
        print()

    # Second pass: Deezer API for non-skipped tracks
    if use_deezer:
        deezer_count = sum(1 for r in results if not r['skip'])
        print(f"\n{C.BOLD}Fetching Deezer metadata for {deezer_count} tracks...{C.END}")
        deezer_done = 0
        deezer_no_match = 0
        for r in results:
            if r['skip']:
                continue
            dz = search_deezer_metadata(r['artist'], r['clean_title'])
            if dz.get('deezer_id'):
                r['deezer'] = dz
                if not r['bpm'] and dz.get('bpm'):
                    r['bpm'] = dz['bpm']
            else:
                # No Deezer match → mark as skipped (won't be written to DB)
                r['skip'] = True
                r['skip_reason'] = 'No Deezer match (required for import)'
                deezer_no_match += 1
            r['_deezer_enabled'] = True
            deezer_done += 1
            if deezer_done % 20 == 0:
                print(f"\r  {deezer_done}/{deezer_count} Deezer lookups done...", end='', flush=True)
            # Rate limit: Deezer allows ~50 req/5s → ~10/s, but we make 2 calls per track
            # So ~5 tracks/s is safe. Sleep 0.2s between tracks.
            time.sleep(0.2)
        print(f"\r  {deezer_done}/{deezer_count} Deezer lookups done.     ")
        if deezer_no_match:
            print(f"  {C.RED}{deezer_no_match} tracks skipped (no Deezer match){C.END}")

    # Print
    print(f"\n{C.BOLD}{'─' * 75}{C.END}")
    for i, r in enumerate(results, 1):
        print_result(r, index=i)
        print()

    print_summary(results, use_db, use_deezer)

    if use_db:
        db_close()


if __name__ == '__main__':
    main()
