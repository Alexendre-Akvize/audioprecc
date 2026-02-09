#!/usr/bin/env python3
"""
REAL import script — downloads tracks from Dropbox, enriches via Deezer,
uploads to S3, writes to DB, and prints created IDs.

Usage:
    python3 run_import.py "/ID 2026/DJ CITY/2022/DECEMBRE" --limit 10
    python3 run_import.py "/ID 2026/DJ CITY/2022/DECEMBRE" --limit 10 --skip-waveform
"""

import re
import os
import sys
import json
import time

# ─── Load .env ───────────────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

import requests

# ─── Colors ──────────────────────────────────────────────────────────────────────
class C:
    HEADER = '\033[95m'; BLUE = '\033[94m'; CYAN = '\033[96m'; GREEN = '\033[92m'
    YELLOW = '\033[93m'; RED = '\033[91m'; BOLD = '\033[1m'; DIM = '\033[2m'; END = '\033[0m'


# ═══════════════════════════════════════════════════════════════════════════════════
# DROPBOX
# ═══════════════════════════════════════════════════════════════════════════════════

DROPBOX_REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', '')
DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')
DROPBOX_APP_SECRET = os.environ.get('DROPBOX_APP_SECRET', '')
DROPBOX_TEAM_MEMBER_ID = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '')
_current_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '')


def refresh_dropbox_token():
    global _current_token
    if not (DROPBOX_REFRESH_TOKEN and DROPBOX_APP_KEY and DROPBOX_APP_SECRET):
        return _current_token
    try:
        resp = requests.post('https://api.dropbox.com/oauth2/token',
            data={'grant_type': 'refresh_token', 'refresh_token': DROPBOX_REFRESH_TOKEN},
            auth=(DROPBOX_APP_KEY, DROPBOX_APP_SECRET), timeout=15)
        if resp.status_code == 200:
            _current_token = resp.json().get('access_token', '')
    except Exception as e:
        print(f"  Token refresh error: {e}")
    return _current_token


def get_token():
    global _current_token
    _current_token = refresh_dropbox_token()
    return _current_token


def get_dropbox_headers():
    token = get_token()
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    if DROPBOX_TEAM_MEMBER_ID:
        headers['Dropbox-API-Select-User'] = DROPBOX_TEAM_MEMBER_ID
    # Namespace
    try:
        acct_resp = requests.post('https://api.dropboxapi.com/2/users/get_current_account',
            headers={'Authorization': f'Bearer {token}',
                     'Dropbox-API-Select-User': DROPBOX_TEAM_MEMBER_ID} if DROPBOX_TEAM_MEMBER_ID else
                    {'Authorization': f'Bearer {token}'}, timeout=15)
        if acct_resp.status_code == 200:
            ns = acct_resp.json().get('root_info', {}).get('root_namespace_id', '')
            if ns:
                headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': ns})
    except:
        pass
    return headers


def list_dropbox_files(folder_path, headers):
    all_files = []
    has_more = True
    cursor = None
    while has_more:
        if cursor:
            resp = requests.post('https://api.dropboxapi.com/2/files/list_folder/continue',
                                 headers=headers, json={'cursor': cursor}, timeout=30)
        else:
            resp = requests.post('https://api.dropboxapi.com/2/files/list_folder',
                                 headers=headers,
                                 json={'path': folder_path, 'recursive': True,
                                       'include_media_info': False, 'include_deleted': False,
                                       'limit': 2000}, timeout=30)
        if resp.status_code != 200:
            print(f"{C.RED}Dropbox error ({resp.status_code}): {resp.text[:200]}{C.END}")
            sys.exit(1)
        result = resp.json()
        for entry in result.get('entries', []):
            if entry.get('.tag') == 'file':
                name = entry.get('name', '').lower()
                if name.endswith('.mp3') or name.endswith('.wav'):
                    all_files.append({
                        'name': entry['name'],
                        'path': entry.get('path_display', ''),
                        'path_lower': entry.get('path_lower', ''),
                        'size': entry.get('size', 0),
                    })
        has_more = result.get('has_more', False)
        cursor = result.get('cursor')
        print(f"\r  Scanning... {len(all_files)} audio files", end='', flush=True)
    print()
    return all_files


def get_temp_download_link(file_path, headers):
    """Get a temporary direct download link from Dropbox."""
    resp = requests.post('https://api.dropboxapi.com/2/files/get_temporary_link',
                         headers=headers, json={'path': file_path}, timeout=15)
    if resp.status_code == 200:
        return resp.json().get('link', '')
    return ''


# ═══════════════════════════════════════════════════════════════════════════════════
# PARSING (same as app.py / test_parsing.py)
# ═══════════════════════════════════════════════════════════════════════════════════

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

TYPE_TO_FILE_FIELD_MAP = {
    'Main': 'trackFile', 'Original Clean': 'originalTrackMp3Clean',
    'Original Dirty': 'originalTrackMp3Dirty', 'Intro': 'intro',
    'Instrumental': 'instru', 'Acapella': 'acapella',
    'Extended': 'extendedTrackMp3',
}


def should_skip(title):
    tl = title.lower()
    for kw in SKIP_KEYWORDS:
        if kw in tl:
            return True, kw
    return False, None


def detect_track_type(title):
    if not title: return None
    tl = title.lower()
    if re.search(r'\(\s*inst(?:rumental)?\s*\)', tl): return 'Instrumental'
    if re.search(r'\(\s*(?:[\w\s]*\s+)?intro(?:\s*-\s*(?:clean|dirty))?\s*\)', tl): return 'Intro'
    if re.search(r'intro\s*-\s*(?:clean|dirty)', tl): return 'Intro'
    if re.search(r'\(\s*clean\s*\)', tl): return 'Original Clean'
    if re.search(r'\(\s*dirty\s*\)', tl): return 'Original Dirty'
    if 'instrumental' in tl or '(inst)' in tl: return 'Instrumental'
    elif 'acapella' in tl or 'a capella' in tl or 'acappella' in tl: return 'Acapella'
    elif 'extended' in tl: return 'Extended'
    return None


def extract_bpm(filename):
    name = os.path.splitext(filename)[0]
    m = re.search(r'(\d{2,3})\s*$', name)
    if m:
        bpm = int(m.group(1))
        if 60 <= bpm <= 200: return bpm
    return None


def clean_title(title):
    if not title: return title
    c = title
    c = re.sub(r'\.(mp3|wav|flac|aac|ogg|m4a)$', '', c, flags=re.IGNORECASE)
    c = re.sub(r'\s+\d{2,3}\s*$', '', c)
    c = re.sub(r'\s*\(\s*(?:clean|dirty|inst(?:rumental)?)\s*\)', '', c, flags=re.IGNORECASE)
    c = re.sub(r'\s*\(\s*(?:[\w\s]*\s+)?intro(?:\s*-\s*(?:clean|dirty))?\s*\)', '', c, flags=re.IGNORECASE)
    if ' - ' in c:
        parts = c.split(' - ', 1)
        if len(parts) > 1: c = parts[1]
    edit_kw = ['remix','edit','intro','outro','transition','hype','club','bootleg','mashup',
               'blend','rework','redrum','flip','version','mix','dub','vip','break intro','slam']
    if ' - ' in c:
        parts = c.rsplit(' - ', 1)
        if len(parts) == 2:
            after = parts[1].lower()
            for kw in edit_kw:
                if kw in after:
                    c = parts[0]; break
    return re.sub(r'\s+', ' ', c).strip()


def extract_artist(filename):
    name = os.path.splitext(filename)[0]
    name = re.sub(r'\s+\d{2,3}\s*$', '', name)
    if ' - ' in name: return name.split(' - ', 1)[0].strip()
    return 'Unknown'


def build_track_id(clean_t, artist, isrc=''):
    combined = f"{artist} - {clean_t}" if artist != 'Unknown' else clean_t
    tid = combined.replace('-', ' ').replace('_', ' ')
    tid = re.sub(r'\s+', ' ', tid).strip().replace(' ', '_')
    tid = re.sub(r'_+', '_', tid)
    return f"{isrc}_{tid}" if isrc else tid


# ═══════════════════════════════════════════════════════════════════════════════════
# DEEZER API
# ═══════════════════════════════════════════════════════════════════════════════════

def search_deezer(artist, title, timeout=10):
    result = {'deezer_id': None, 'isrc': '', 'bpm': None, 'title': '', 'artist': '',
              'album': '', 'label': '', 'release_date': '', 'genre': '', 'cover_url': '',
              'duration': None, 'explicit': False}
    if not artist or not title: return result
    clean_a = re.sub(r'\s*(feat\.?|ft\.?|featuring)\s+.*$', '', artist, flags=re.IGNORECASE).strip()
    clean_t = re.sub(r'\s*\(.*?\)', '', title).strip()
    clean_t = re.sub(r'\s*-\s*$', '', clean_t).strip()
    try:
        url = 'https://api.deezer.com/search'
        params = {'q': f'artist:"{clean_a}" track:"{clean_t}"', 'limit': 5}
        resp = requests.get(url, params=params, timeout=timeout)
        tracks = resp.json().get('data', []) if resp.status_code == 200 else []
        if not tracks:
            params = {'q': f'{clean_a} {clean_t}', 'limit': 5}
            resp = requests.get(url, params=params, timeout=timeout)
            tracks = resp.json().get('data', []) if resp.status_code == 200 else []
        if not tracks: return result
        t = tracks[0]
        result['deezer_id'] = t.get('id')
        result['title'] = t.get('title', '')
        result['artist'] = t.get('artist', {}).get('name', '')
        result['album'] = t.get('album', {}).get('title', '')
        result['cover_url'] = t.get('album', {}).get('cover_xl', '')
        result['duration'] = t.get('duration')
        result['explicit'] = t.get('explicit_lyrics', False)
        if result['deezer_id']:
            dr = requests.get(f'https://api.deezer.com/track/{result["deezer_id"]}', timeout=timeout)
            if dr.status_code == 200:
                d = dr.json()
                result['isrc'] = d.get('isrc', '')
                result['bpm'] = d.get('bpm') if d.get('bpm') and d['bpm'] > 0 else None
                ad = d.get('album', {})
                result['label'] = ad.get('label', '')
                result['release_date'] = ad.get('release_date', '')
                if not result['cover_url']: result['cover_url'] = ad.get('cover_xl', '')
                genres = ad.get('genres', {}).get('data', [])
                if genres: result['genre'] = genres[0].get('name', '')
    except:
        pass
    return result


# ═══════════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════════

def main():
    folder_path = '/ID 2026/DJ CITY/2022/DECEMBRE'
    limit = 10
    skip_waveform = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--limit' and i + 1 < len(args):
            limit = int(args[i + 1]); i += 2
        elif args[i] == '--skip-waveform':
            skip_waveform = True; i += 1
        elif args[i] in ('-h', '--help'):
            print(__doc__); sys.exit(0)
        else:
            folder_path = args[i]; i += 1

    print(f"\n{C.BOLD}═══ REAL IMPORT (limit: {limit} tracks) ═══{C.END}")
    print(f"  Folder: {folder_path}")
    print(f"  Skip waveform: {skip_waveform}\n")

    # 1. Connect to Dropbox
    print(f"{C.BOLD}1. Connecting to Dropbox...{C.END}")
    dbx_headers = get_dropbox_headers()

    # 2. List files
    print(f"{C.BOLD}2. Listing files...{C.END}")
    all_files = list_dropbox_files(folder_path, dbx_headers)
    print(f"  {C.GREEN}{len(all_files)} audio files found{C.END}")

    # 3. Filter & pick tracks
    print(f"\n{C.BOLD}3. Filtering...{C.END}")
    candidates = []
    for f in sorted(all_files, key=lambda x: x['name'].lower()):
        skip, kw = should_skip(f['name'])
        if skip:
            continue
        candidates.append(f)

    print(f"  {len(candidates)} passed keyword filter (from {len(all_files)} total)")

    # 4. Deezer check + pick first N with matches
    print(f"\n{C.BOLD}4. Deezer lookup (finding {limit} tracks with matches)...{C.END}")
    to_import = []
    checked = 0
    for f in candidates:
        if len(to_import) >= limit:
            break
        artist = extract_artist(f['name'])
        ct = clean_title(f['name'])
        dz = search_deezer(artist, ct)
        checked += 1
        if dz.get('deezer_id'):
            f['_artist'] = artist
            f['_clean_title'] = ct
            f['_bpm'] = extract_bpm(f['name'])
            f['_type'] = detect_track_type(f['name'])
            f['_deezer'] = dz
            to_import.append(f)
            print(f"  {C.GREEN}✓{C.END} [{len(to_import)}/{limit}] {f['name']}")
            print(f"          Deezer: {dz['artist']} - {dz['title']}  ISRC={dz['isrc']}")
        else:
            print(f"  {C.DIM}✗ {f['name']} (no Deezer match){C.END}")
        time.sleep(0.2)

    if not to_import:
        print(f"{C.RED}No importable tracks found.{C.END}")
        sys.exit(1)

    print(f"\n  {C.GREEN}{len(to_import)} tracks ready to import{C.END} (checked {checked})")

    # 5. Import to database
    print(f"\n{C.BOLD}5. Importing to database...{C.END}")

    # Import the database service (Prisma + S3)
    from database_service import save_track_to_database, check_database_connection

    if not check_database_connection():
        print(f"{C.RED}Database connection failed!{C.END}")
        sys.exit(1)
    print(f"  {C.GREEN}Database connected{C.END}")

    results = []
    for idx, f in enumerate(to_import, 1):
        print(f"\n{'─' * 60}")
        print(f"{C.BOLD}[{idx}/{len(to_import)}] {f['name']}{C.END}")

        artist = f['_artist']
        ct = f['_clean_title']
        bpm = f['_bpm']
        track_type = f['_type'] or 'Main'
        dz = f['_deezer']

        # Enrich from Deezer
        isrc = dz.get('isrc', '')
        if not bpm and dz.get('bpm'):
            bpm = dz['bpm']
        album = dz.get('album', '')
        genre = dz.get('genre', '')
        label = dz.get('label', '')
        release_date = dz.get('release_date', '')
        cover_url = dz.get('cover_url', '')

        # Get temp download link from Dropbox
        print(f"  📥 Getting Dropbox download link...")
        file_url = get_temp_download_link(f['path_lower'] or f['path'], dbx_headers)
        if not file_url:
            print(f"  {C.RED}Failed to get download link, skipping{C.END}")
            results.append({'filename': f['name'], 'error': 'No download link'})
            continue
        print(f"  {C.GREEN}Got download link{C.END}")

        # Parse release date to timestamp
        date_sortie = 0
        if release_date:
            try:
                from datetime import datetime
                date_obj = datetime.strptime(release_date[:10], '%Y-%m-%d')
                date_sortie = int(date_obj.timestamp())
            except:
                pass

        # Build track ID
        track_id = build_track_id(ct, artist, isrc)

        # Build track title
        title_lower = (ct or '').lower()
        if track_type.lower() in title_lower:
            track_title = ct
        elif track_type != 'Main':
            track_title = f"{ct} - {track_type}"
        else:
            track_title = f"{ct} - Main"

        # Map field
        db_field_type = track_type

        # Build track_data dict (same format as upload_direct)
        track_data = {
            'Type': db_field_type,
            'Format': 'WAV' if f['name'].lower().endswith('.wav') else 'MP3',
            'Titre': track_title,
            'Artiste': artist,
            'Fichiers': file_url,
            'Univers': '',
            'Mood': '',
            'Style': genre,
            'Album': album,
            'Label': label,
            'Sous-label': '',
            'Date de sortie': date_sortie,
            'BPM': bpm if bpm else 0,
            'Artiste original': artist,
            'Url': cover_url,
            'ISRC': isrc,
            'TRACK_ID': track_id,
            '_force_cover_replace': True,
        }

        print(f"  Type: {track_type}  →  Field: {TYPE_TO_FILE_FIELD_MAP.get(track_type, 'trackFile')}")
        print(f"  Title: {track_title}")
        print(f"  Artist: {artist}")
        print(f"  BPM: {bpm}  ISRC: {isrc}")
        print(f"  Cover: {cover_url[:60]}..." if cover_url else "  Cover: (none)")

        # Save to database (handles S3 upload, cover upload, Prisma write)
        print(f"  💾 Saving to database...")
        try:
            result = save_track_to_database(track_data, skip_waveform=skip_waveform)
            if 'error' in result:
                print(f"  {C.RED}ERROR: {result['error']}{C.END}")
                results.append({'filename': f['name'], 'error': result['error']})
            else:
                db_id = result.get('id', '???')
                action = result.get('action', 'unknown')
                print(f"  {C.GREEN}✅ {action.upper()}: ID = {db_id}{C.END}")
                results.append({
                    'filename': f['name'],
                    'id': db_id,
                    'trackId': result.get('trackId', track_id),
                    'action': action,
                    'type': track_type,
                    'artist': artist,
                    'title': ct,
                    'isrc': isrc,
                })
        except Exception as e:
            print(f"  {C.RED}EXCEPTION: {e}{C.END}")
            import traceback; traceback.print_exc()
            results.append({'filename': f['name'], 'error': str(e)})

    # ═══ Summary ═══
    print(f"\n{'=' * 60}")
    print(f"{C.BOLD}  IMPORT RESULTS{C.END}")
    print(f"{'=' * 60}")

    created = [r for r in results if r.get('action') == 'created']
    updated = [r for r in results if r.get('action') == 'updated']
    errors = [r for r in results if r.get('error')]

    print(f"  {C.GREEN}Created: {len(created)}{C.END}")
    print(f"  {C.YELLOW}Updated: {len(updated)}{C.END}")
    print(f"  {C.RED}Errors:  {len(errors)}{C.END}")

    if created or updated:
        print(f"\n  {C.BOLD}Database IDs:{C.END}")
        for r in created + updated:
            action_badge = f"{C.GREEN}NEW{C.END}" if r['action'] == 'created' else f"{C.YELLOW}UPD{C.END}"
            print(f"    {action_badge}  {C.BOLD}{r['id']}{C.END}")
            print(f"         {r['artist']} - {r['title']}  [{r['type']}]  ISRC={r['isrc']}")

    if errors:
        print(f"\n  {C.BOLD}Errors:{C.END}")
        for r in errors:
            print(f"    {C.RED}✗{C.END} {r['filename']}: {r['error']}")

    # Print just the IDs for easy copy
    all_ids = [r['id'] for r in created + updated if r.get('id')]
    if all_ids:
        print(f"\n  {C.BOLD}IDs (copy-paste):{C.END}")
        for i in all_ids:
            print(f"    {i}")

    print(f"{'=' * 60}\n")


if __name__ == '__main__':
    main()
