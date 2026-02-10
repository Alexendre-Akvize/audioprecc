"""
Metadata service for IDByRivoli.

Title cleaning, Deezer API lookups, ID3 tag management, and API export.
"""
import os
import re
import json
import requests
from datetime import datetime

from mutagen.mp3 import MP3
from mutagen.id3 import (
    ID3, TIT2, TPE1, APIC, TALB, TDRC, TRCK, TCON,
    TBPM, TSRC, TLEN, TPUB, WXXX, TXXX,
)

from config import (
    BASE_DIR,
    USE_DATABASE_MODE,
    API_ENDPOINT,
    API_KEY,
    CURRENT_HOST_URL,
    bulk_import_state,
)
from utils.file_utils import format_artists, get_parent_label, clean_filename


# ---------------------------------------------------------------------------
# Logging helper – avoids circular import with queue_service
# ---------------------------------------------------------------------------
def _log_message(message, session_id=None):
    """Fallback-safe wrapper around queue_service.log_message."""
    try:
        from services.queue_service import log_message
        log_message(message, session_id)
    except ImportError:
        print(message)


# =============================================================================
# TITLE DETECTION / CLEANING CONSTANTS
# =============================================================================

# Keywords that should cause track to be SKIPPED/DELETED (case-insensitive)
SKIP_KEYWORDS = [
    'rework', 're-work', 'boot', 'bootleg', 'mashup', 'mash-up', 'mash up',
    'riddim', 'ridim', 'redrum', 're-drum', 'transition',
    'hype', 'throwback hype', 'wordplay', 'tonalplay', 'tonal play', 'toneplay',
    'beat intro', 'segway', 'segue', 'edit',
    'blend', 'anthem', 'club', 'halloween', 'christmas', 'easter',
    'countdown', 'private', 'party break',
    'sample', 'chill mix', 'kidcutup', 'kid cut up',
    'bounce back', 'chorus in', 'orchestral',
    'da phonk', 'daphonk', 'epice intro', 'epic intro'
]

# Note: 'Remix' is a special case - we might want to keep some remixes
# Add it separately so it can be easily toggled
SKIP_REMIX = True
if SKIP_REMIX:
    SKIP_KEYWORDS.append('remix')

# DJ/Pool names to replace with "ID By Rivoli" (case-insensitive)
DJ_NAMES_TO_REPLACE = [
    'BPM Supreme', 'Bpmsupreme', 'BPMSupreme',
    'Hh', 'HH',
    'Heavy Hits', 'HeavyHits', 'Heavy-Hits',
    'Dj city', 'DJcity', 'DJ City',
    'HMC',
    'FuviClan', 'Fuvi Clan', 'Fuvi-Clan',
    'Bangerz Army', 'BangerzArmy', 'Bangerz-Army',
    'BarBangerz', 'Bar Bangerz', 'Bar-Bangerz',
    'Beatfreakz', 'Beat Freakz', 'Beat-Freakz',
    'Beatport',
    'Bpm Latino', 'Bpmlatino', 'BPM Latino', 'BPMLatino',
    'Club Killers', 'Clubkillers', 'Club-Killers',
    'Crack4', 'Crack 4',
    'Crooklyn Clan', 'CrooklynClan', 'Crooklyn-Clan',
    'Da Throwbackz', 'DaThrowbackz', 'Da-Throwbackz',
    'Direct Music Service', 'DirectMusicService', 'DMS',
    'Dj BeatBreaker', 'DjBeatBreaker', 'DJ BeatBreaker', 'DJ Beat Breaker',
    'DMC',
    'Doing The Damage', 'DoingTheDamage', 'Doing-The-Damage',
    'DJ Precise', 'DJPrecise',
    'DJ Snake', 'DJSnake',
    'X-Mix', 'XMix', 'X Mix',
    'Dirty Dutch', 'DirtyDutch',
    'Promo Only', 'PromoOnly',
    'DJ Tools', 'DJTools',
    'Select Mix', 'SelectMix',
    'Ultimix',
    'Funkymix', 'Funky Mix',
]

# Format standardization mappings
FORMAT_MAPPINGS = {
    'quick hit': 'Short',
    'quickhit': 'Short',
    'quick-hit': 'Short',
    'cut': 'Short',
    'snip': 'Short',
    'acapella intro': 'Acap In',
    'acap intro': 'Acap In',
    'acapella outro': 'Acap Out',
    'acap outro': 'Acap Out',
    'acapella in': 'Acap In',
    'acapella out': 'Acap Out',
    'a cappella': 'Acapella',
    'a capella': 'Acapella',
    'acappella': 'Acapella',
    'acap': 'Acapella',
    'inst': 'Instrumental',
}

# Track types/versions to generate
TRACK_VERSIONS = ['Dirty', 'Clean']


# =============================================================================
# TITLE DETECTION FUNCTIONS
# =============================================================================

def detect_track_type_from_title(title):
    """
    Detect track type from title/filename.
    Returns the detected type string or None if regular track.
    Case-insensitive matching.
    
    Supports both parenthetical (DJ City) and square bracket (DJ pool) formats:
    - (Clean), [Clean] → 'Original Clean'
    - (Dirty), [Dirty] → 'Original Dirty'
    - (Intro), [Intro Clean], [Intro Dirty] → 'Intro'
    - (Inst), [Instrumental] → 'Instrumental'
    - [Dirty Acapella], [Clean Acapella] → 'Acapella'
    - [Quick Hit Clean], [Quick Hit Dirty] → 'Short'
    - (Extended), [Extended] → 'Extended'
    
    Detected types (mapped to database fields via TYPE_TO_FILE_FIELD_MAP):
    - 'Instrumental' → instru
    - 'Acapella' → acapella
    - 'Extended' → extendedTrackMp3
    - 'Original Clean' → originalTrackMp3Clean
    - 'Original Dirty' → originalTrackMp3Dirty
    - 'Intro' → intro
    - 'Short' → short
    """
    if not title:
        return None
    
    title_lower = title.lower()
    
    # === Bracketed/Parenthetical markers (most specific, check first) ===
    # Support both () and [] formats: [\(\[] for open, [\)\]] for close
    
    # [Inst] or [Instrumental] or (Inst) or (Instrumental)
    if re.search(r'[\(\[]\s*inst(?:rumental)?\s*[\)\]]', title_lower):
        return 'Instrumental'
    
    # [Dirty Acapella] or [Clean Acapella] or (Dirty Acapella) or (Clean Acapella)
    if re.search(r'[\(\[]\s*(?:dirty|clean)\s+acapella\s*[\)\]]', title_lower):
        return 'Acapella'
    
    # [Acapella] or (Acapella) or [Acap] or (Acap)
    if re.search(r'[\(\[]\s*acap(?:ella)?\s*[\)\]]', title_lower):
        return 'Acapella'
    
    # [Intro Clean] or [Intro Dirty] or [Intro] or (Intro) etc.
    if re.search(r'[\(\[]\s*(?:[\w\s]*\s+)?intro(?:\s+(?:clean|dirty))?(?:\s*-\s*(?:clean|dirty))?\s*[\)\]]', title_lower):
        return 'Intro'
    
    # Intro - Clean or Intro - Dirty without brackets
    if re.search(r'intro\s*-\s*(?:clean|dirty)', title_lower):
        return 'Intro'
    
    # [Quick Hit Clean] or [Quick Hit Dirty] or [Quick Hit] or (Quick Hit)
    if re.search(r'[\(\[]\s*quick\s*hit(?:\s+(?:clean|dirty))?\s*[\)\]]', title_lower):
        return 'Short'
    
    # [Short] or (Short)
    if re.search(r'[\(\[]\s*short\s*[\)\]]', title_lower):
        return 'Short'
    
    # [Extended] or (Extended)
    if re.search(r'[\(\[]\s*extended\s*[\)\]]', title_lower):
        return 'Extended'
    
    # [Clean] or (Clean) - standalone clean version marker
    if re.search(r'[\(\[]\s*clean\s*[\)\]]', title_lower):
        return 'Original Clean'
    
    # [Dirty] or (Dirty) - standalone dirty version marker
    if re.search(r'[\(\[]\s*dirty\s*[\)\]]', title_lower):
        return 'Original Dirty'
    
    # === General keywords (less specific, no brackets needed) ===
    if 'instrumental' in title_lower or '(inst)' in title_lower or '[inst]' in title_lower:
        return 'Instrumental'
    elif 'acapella' in title_lower or 'a capella' in title_lower or 'acappella' in title_lower or re.search(r'\bacap\b', title_lower):
        return 'Acapella'
    elif 'extended' in title_lower:
        return 'Extended'
    
    return None


def extract_bpm_from_filename(filename):
    """
    Extract BPM from trailing number in filename (before extension).
    DJ City files have format: 'Artist - Title (Version) BPM.mp3'
    
    Examples:
    - 'A-Trak - Bubble Guts - Braxe & Falcon Remix 122.mp3' → 122
    - 'Akon Ft. John Mamann - Tt Freak (Clean) 123.mp3' → 123
    - 'Alcyone - Trompeta Y Fiesta - Luis R 100-130 Transition 130.mp3' → 130
    
    Returns BPM as int, or None if not found.
    """
    # Remove extension
    name = os.path.splitext(filename)[0]
    # Match trailing 2-3 digit number (BPM typically 60-200)
    match = re.search(r'(\d{2,3})\s*$', name)
    if match:
        bpm = int(match.group(1))
        if 60 <= bpm <= 200:
            return bpm
    return None


def strip_trailing_bpm_and_key(title):
    """
    Remove trailing BPM number and/or Camelot key from a title.
    DJ pools (DJcity, BPM Supreme, etc.) append these at the end of titles.
    
    Pattern: Title [Version] CamelotKey BPM
    
    Examples:
    - 'Hot Spot [Dirty] 10A 93'           → 'Hot Spot [Dirty]'
    - 'What\\'s Not To Love [Clean] 2A 98'  → 'What\\'s Not To Love [Clean]'
    - 'Hot Spot [Dirty Acapella] 1B'       → 'Hot Spot [Dirty Acapella]'
    - 'Hot Spot [Instrumental] 10A'        → 'Hot Spot [Instrumental]'
    - 'Holiday [Quick Hit Clean] 7A 102'   → 'Holiday [Quick Hit Clean]'
    - 'Fine Wine & Hennessy (Intro) 102'   → 'Fine Wine & Hennessy (Intro)'
    """
    if not title:
        return title
    
    # Combined pattern: optional Camelot key (1A-12A, 1B-12B) + optional BPM (2-3 digits)
    # Matches: "10A 93", "2A 98", "1B", "10A", "93", "102", etc.
    cleaned = re.sub(r'\s+\d{1,2}[ABab]\s+\d{2,3}\s*$', '', title)    # "10A 93" at end
    cleaned = re.sub(r'\s+\d{2,3}\s*$', '', cleaned)                   # standalone BPM at end
    cleaned = re.sub(r'\s+\d{1,2}[ABab]\s*$', '', cleaned)             # standalone Camelot key at end
    
    return cleaned.strip()


def clean_detected_type_from_title(title, detected_type=None):
    """
    Remove detected type markers and version info from title for use as base name.
    Called after detect_track_type_from_title() to strip type-related content.
    
    Removes:
    - (Clean), (Dirty), (Inst), (Instrumental)
    - (Djcity Intro - Clean), (XXX Intro), (Intro - Clean), (Intro - Dirty)
    - Trailing BPM numbers
    - Artist name (everything before first " - ")
    - Remixer/DJ edit sections after second " - " when they contain known edit keywords
    
    Examples:
    - 'Akon Ft. John Mamann - Tt Freak (Clean) 123' → 'Tt Freak'
    - 'Alcyone - Trompeta Y Fiesta (Djcity Intro) 130' → 'Trompeta Y Fiesta'
    - 'Bad Bunny - Party - Rob Dvs Hip Hop Hype Intro (Dirty) 100' → 'Party'
    """
    if not title:
        return title
    
    cleaned = title
    
    # Remove file extension if present
    cleaned = re.sub(r'\.(mp3|wav|flac|aac|ogg|m4a)$', '', cleaned, flags=re.IGNORECASE)
    
    # Remove trailing BPM and/or Camelot key (e.g., "10A 93", "1B", "102")
    cleaned = strip_trailing_bpm_and_key(cleaned)
    
    # Remove "Radio Edit" markers - this is the main version, not a variant
    # (Radio Edit), [Radio Edit], Radio-Edit, Radio Edit
    cleaned = re.sub(r'\s*[\(\[]\s*radio\s+edit\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s*-\s*Radio[\s-]+Edit', '', cleaned, flags=re.IGNORECASE)
    
    # Remove parenthetical and square bracket version/type markers
    # (Clean), (Dirty), (Inst), (Instrumental), [Clean], [Dirty], [Instrumental]
    cleaned = re.sub(r'\s*[\(\[]\s*(?:clean|dirty|inst(?:rumental)?)\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    
    # [Dirty Acapella], [Clean Acapella], (Dirty Acapella), (Dirty Acap)
    cleaned = re.sub(r'\s*[\(\[]\s*(?:dirty|clean)\s+acap(?:ella)?\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    
    # [Acapella], (Acapella), [Acap], (Acap)
    cleaned = re.sub(r'\s*[\(\[]\s*acap(?:ella)?\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    
    # [Quick Hit Clean], [Quick Hit Dirty], [Quick Hit], (Quick Hit)
    cleaned = re.sub(r'\s*[\(\[]\s*quick\s*hit(?:\s+(?:clean|dirty))?\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    
    # [Short], (Short)
    cleaned = re.sub(r'\s*[\(\[]\s*short\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    
    # [Extended], (Extended)
    cleaned = re.sub(r'\s*[\(\[]\s*extended\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    
    # (Djcity Intro - Clean), (Intro - Dirty), (Intro), [Intro Clean], [Intro Dirty], [Intro]
    cleaned = re.sub(r'\s*[\(\[]\s*(?:[\w\s]*\s+)?intro(?:\s+(?:clean|dirty))?(?:\s*-\s*(?:clean|dirty))?\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    
    # Remove artist name (everything before first " - ")
    if ' - ' in cleaned:
        parts = cleaned.split(' - ', 1)
        if len(parts) > 1:
            cleaned = parts[1]
    
    # Remove remixer/DJ edit section (content after last " - " containing edit keywords)
    # e.g., "Tt Freak - Rob Dvs Hip Hop Hype Intro" → "Tt Freak"
    edit_keywords = [
        'remix', 'edit', 'intro', 'outro', 'transition', 'hype', 'club',
        'bootleg', 'mashup', 'blend', 'rework', 'redrum', 'flip',
        'version', 'mix', 'dub', 'vip', 'break intro', 'slam'
    ]
    if ' - ' in cleaned:
        parts = cleaned.rsplit(' - ', 1)
        if len(parts) == 2:
            after_dash = parts[1].lower()
            # Check if the part after the last " - " contains an edit keyword
            for keyword in edit_keywords:
                if keyword in after_dash:
                    cleaned = parts[0]
                    break
    
    # Clean up whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned


# =============================================================================
# DEEZER API - Metadata & Cover Art Lookup
# =============================================================================

def search_deezer_metadata(artist, title, timeout=10):
    """
    Search Deezer API for track metadata: ISRC, BPM, key, genre, label, album,
    release date, and cover art URL.
    
    Uses two API calls:
      1. /search?q=artist:"X" track:"Y"  → find the Deezer track ID
      2. /track/{id}                      → get full details (ISRC, BPM, etc.)
    
    Returns dict with all found metadata, or empty dict on failure.
    Free API, no auth needed, rate limit ~50 req/5s.
    """
    import requests as _req

    result = {
        'deezer_id': None,
        'isrc': '',
        'bpm': None,
        'title': '',
        'artist': '',
        'album': '',
        'label': '',
        'release_date': '',
        'genre': '',
        'cover_url': '',          # album.cover_xl (1000×1000)
        'cover_url_medium': '',   # album.cover_big (500×500)
        'duration': None,
        'explicit': False,
        'deezer_link': '',
        'match_score': 0.0,       # How well the result matches the query (0-1)
    }

    if not artist or not title:
        return result

    # Clean search terms - remove feat/ft. for better matching
    clean_artist = re.sub(r'\s*(feat\.?|ft\.?|featuring)\s+.*$', '', artist, flags=re.IGNORECASE).strip()
    clean_title = re.sub(r'\s*\(.*?\)', '', title).strip()  # remove parenthetical markers
    clean_title = re.sub(r'\s*-\s*$', '', clean_title).strip()

    try:
        # Step 1: Search for the track
        search_url = 'https://api.deezer.com/search'
        params = {'q': f'artist:"{clean_artist}" track:"{clean_title}"', 'limit': 5}
        resp = _req.get(search_url, params=params, timeout=timeout)

        if resp.status_code != 200:
            return result

        data = resp.json()
        tracks = data.get('data', [])

        if not tracks:
            # Fallback: simpler search without strict artist/track syntax
            params = {'q': f'{clean_artist} {clean_title}', 'limit': 5}
            resp = _req.get(search_url, params=params, timeout=timeout)
            if resp.status_code == 200:
                tracks = resp.json().get('data', [])

        if not tracks:
            return result

        # ── Score each result and pick the best match ──
        def _normalize(s):
            """Lowercase, strip accents, remove punctuation for comparison."""
            import unicodedata
            s = unicodedata.normalize('NFKD', str(s).lower())
            s = ''.join(c for c in s if not unicodedata.combining(c))
            s = re.sub(r'[^a-z0-9\s]', '', s)
            return ' '.join(s.split())

        def _word_overlap_score(a, b):
            """Return 0-1 score based on word overlap (Jaccard-like)."""
            words_a = set(_normalize(a).split())
            words_b = set(_normalize(b).split())
            if not words_a or not words_b:
                return 0.0
            intersection = words_a & words_b
            union = words_a | words_b
            return len(intersection) / len(union) if union else 0.0

        def _contains_score(query, candidate):
            """Return 1.0 if normalized query is contained in candidate or vice versa."""
            nq = _normalize(query)
            nc = _normalize(candidate)
            if nq in nc or nc in nq:
                return 1.0
            return 0.0

        def _score_track(t, searched_artist, searched_title):
            """Score a Deezer result (0-1) against searched artist+title."""
            t_artist = t.get('artist', {}).get('name', '')
            t_title = t.get('title', '')

            # Artist score: best of word-overlap and contains
            artist_score = max(
                _word_overlap_score(searched_artist, t_artist),
                _contains_score(searched_artist, t_artist)
            )
            # Title score: best of word-overlap and contains
            title_score = max(
                _word_overlap_score(searched_title, t_title),
                _contains_score(searched_title, t_title)
            )
            # Combined: title matters more (60%) since covers depend on it
            return artist_score * 0.4 + title_score * 0.6

        # Score all candidates and pick the best
        MINIMUM_MATCH_SCORE = 0.50  # Below this, treat as no match — track will be skipped entirely
        best_track = None
        best_score = -1.0
        for t in tracks:
            s = _score_track(t, clean_artist, clean_title)
            t_artist = t.get('artist', {}).get('name', '')
            t_title = t.get('title', '')
            print(f"   🎯 Deezer candidate: '{t_artist}' - '{t_title}' → score {s:.2f}")
            if s > best_score:
                best_score = s
                best_track = t

        if best_score < MINIMUM_MATCH_SCORE or best_track is None:
            print(f"   ❌ No Deezer result above threshold ({best_score:.2f} < {MINIMUM_MATCH_SCORE})")
            return result

        track = best_track
        print(f"   ✅ Best Deezer match (score {best_score:.2f}): '{track.get('artist', {}).get('name', '')}' - '{track.get('title', '')}'")
        deezer_id = track.get('id')
        result['deezer_id'] = deezer_id
        result['match_score'] = best_score
        result['title'] = track.get('title', '')
        result['artist'] = track.get('artist', {}).get('name', '')
        result['album'] = track.get('album', {}).get('title', '')
        result['cover_url'] = track.get('album', {}).get('cover_xl', '')
        result['cover_url_medium'] = track.get('album', {}).get('cover_big', '')
        result['duration'] = track.get('duration')
        result['explicit'] = track.get('explicit_lyrics', False)
        result['deezer_link'] = track.get('link', '')

        # Step 2: Get full track details (ISRC, BPM, label, genre, release date)
        if deezer_id:
            detail_resp = _req.get(f'https://api.deezer.com/track/{deezer_id}', timeout=timeout)
            if detail_resp.status_code == 200:
                detail = detail_resp.json()

                result['isrc'] = detail.get('isrc', '')
                result['bpm'] = detail.get('bpm') if detail.get('bpm') and detail.get('bpm') > 0 else None

                # Album-level details
                album_data = detail.get('album', {})
                if album_data:
                    result['label'] = album_data.get('label', '')
                    result['release_date'] = album_data.get('release_date', '')
                    if not result['cover_url']:
                        result['cover_url'] = album_data.get('cover_xl', '')
                    if not result['cover_url_medium']:
                        result['cover_url_medium'] = album_data.get('cover_big', '')

                    # Genre from album
                    genres = album_data.get('genres', {}).get('data', [])
                    if genres:
                        result['genre'] = genres[0].get('name', '')

    except Exception as e:
        print(f"   ⚠️ Deezer API error: {e}")

    return result


def get_deezer_cover_url(artist, title, timeout=10):
    """
    Quick helper: just get the cover art URL from Deezer.
    Returns the cover_xl URL (1000×1000) or empty string.
    """
    meta = search_deezer_metadata(artist, title, timeout=timeout)
    return meta.get('cover_url', '')


# =============================================================================
# SKIP / CLEAN / METADATA EXTRACTION
# =============================================================================

def should_skip_track(title):
    """
    Check if track should be skipped based on keywords in title.
    Returns (should_skip: bool, reason: str or None)
    """
    if not title:
        return False, None
    
    title_lower = title.lower()
    
    # "Radio Edit" = main version, must NOT be skipped by the 'edit' keyword
    # Remove "radio edit" before checking skip keywords so it doesn't false-match
    check_title = re.sub(r'radio\s+edit', '', title_lower)
    
    for keyword in SKIP_KEYWORDS:
        if keyword.lower() in check_title:
            return True, f"Contains '{keyword}'"
    
    return False, None


def clean_track_title(title):
    """
    Clean track title:
    1. Remove artist name (everything before " - ")
    2. Remove BPM numbers at the end
    3. Replace DJ/pool names with "ID By Rivoli"
    4. Standardize formats (Quick Hit -> Short, etc.)
    5. Add "ID By Rivoli" in format parentheses
    6. Clean up parentheses
    
    Example: "Afro B & Slim Jxmmi - Fine Wine & Hennessy (Intro) 102"
          -> "Fine Wine & Hennessy (ID By Rivoli Intro)"
    
    Returns cleaned title string.
    """
    if not title:
        return title
    
    cleaned = title
    
    # 1. Remove artist name (everything before " - ")
    if ' - ' in cleaned:
        parts = cleaned.split(' - ', 1)
        if len(parts) > 1:
            cleaned = parts[1]  # Keep only the track title part
    
    # 2. Remove trailing BPM and/or Camelot key (e.g., "10A 93", "1B", "102")
    cleaned = strip_trailing_bpm_and_key(cleaned)
    # Also handle BPM/key before parenthesis: "Title 102 (Intro)" -> "Title (Intro)"
    cleaned = re.sub(r'\s+\d{1,2}[ABab]\s+\d{2,3}\s+(\([^)]+\))\s*$', r' \1', cleaned)
    cleaned = re.sub(r'\s+\d{2,3}\s+(\([^)]+\))\s*$', r' \1', cleaned)
    cleaned = re.sub(r'\s+\d{1,2}[ABab]\s+(\([^)]+\))\s*$', r' \1', cleaned)
    
    # 2b. Remove "Radio Edit" markers (Radio Edit = main version, not a variant)
    cleaned = re.sub(r'\s*[\(\[]\s*radio\s+edit\s*[\)\]]', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s*-\s*Radio[\s-]+Edit', '', cleaned, flags=re.IGNORECASE)
    
    # 3. Replace DJ/pool names with "ID By Rivoli" (case-insensitive)
    for dj_name in DJ_NAMES_TO_REPLACE:
        # Create case-insensitive pattern
        pattern = re.compile(re.escape(dj_name), re.IGNORECASE)
        cleaned = pattern.sub('ID By Rivoli', cleaned)
    
    # 4. Apply format mappings (case-insensitive)
    for old_format, new_format in FORMAT_MAPPINGS.items():
        pattern = re.compile(re.escape(old_format), re.IGNORECASE)
        cleaned = pattern.sub(new_format, cleaned)
    
    # 5. Add "ID By Rivoli" to format parentheses that don't have it
    # Patterns like (Intro), (Outro), (Short), (Acap In), etc.
    format_keywords = [
        'Intro', 'Outro', 'Short', 'Acap In', 'Acap Out', 
        'Acapella', 'Instrumental', 'Extended', 'Main', 
        'Verse', 'Hook', 'Chorus', 'Break', 'Drop'
    ]
    
    for keyword in format_keywords:
        # Match (keyword) without "ID By Rivoli" already in it
        # Pattern: (keyword) or (Dirty keyword) or (Clean keyword)
        pattern = re.compile(
            r'\(\s*(?!ID By Rivoli)(' + re.escape(keyword) + r')\s*\)',
            re.IGNORECASE
        )
        cleaned = pattern.sub(r'(ID By Rivoli \1)', cleaned)
        
        # Also handle (Dirty Intro) -> (ID By Rivoli Intro) (Dirty)
        pattern_dirty = re.compile(
            r'\(\s*(?!ID By Rivoli)(Dirty)\s+(' + re.escape(keyword) + r')\s*\)',
            re.IGNORECASE
        )
        cleaned = pattern_dirty.sub(r'(ID By Rivoli \2) (Dirty)', cleaned)
        
        pattern_clean = re.compile(
            r'\(\s*(?!ID By Rivoli)(Clean)\s+(' + re.escape(keyword) + r')\s*\)',
            re.IGNORECASE
        )
        cleaned = pattern_clean.sub(r'(ID By Rivoli \2) (Clean)', cleaned)
    
    # 6. Clean up double spaces
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Clean up empty parentheses
    cleaned = re.sub(r'\(\s*\)', '', cleaned)
    
    # Clean up double "ID By Rivoli"
    cleaned = re.sub(r'ID By Rivoli\s+ID By Rivoli', 'ID By Rivoli', cleaned, flags=re.IGNORECASE)
    
    # Extract and reorganize version info (Dirty/Clean)
    cleaned = reorganize_version_info(cleaned)
    
    return cleaned.strip()


def reorganize_version_info(title):
    """
    Reorganize version info so Dirty/Clean is at the end in its own parentheses.
    Example: (Hh Dirty Intro) -> (ID By Rivoli Intro) (Dirty)
    """
    if not title:
        return title
    
    result = title
    version_found = None
    
    # Check for Dirty/Clean inside parentheses and extract them
    dirty_pattern = re.compile(r'\(\s*([^)]*?)\s*dirty\s*([^)]*?)\s*\)', re.IGNORECASE)
    clean_pattern = re.compile(r'\(\s*([^)]*?)\s*clean\s*([^)]*?)\s*\)', re.IGNORECASE)
    
    # Check for Dirty
    dirty_match = dirty_pattern.search(result)
    if dirty_match:
        before = dirty_match.group(1).strip()
        after = dirty_match.group(2).strip()
        content = f"{before} {after}".strip()
        if content:
            result = dirty_pattern.sub(f'({content})', result)
        else:
            result = dirty_pattern.sub('', result)
        version_found = 'Dirty'
    
    # Check for Clean
    clean_match = clean_pattern.search(result)
    if clean_match:
        before = clean_match.group(1).strip()
        after = clean_match.group(2).strip()
        content = f"{before} {after}".strip()
        if content:
            result = clean_pattern.sub(f'({content})', result)
        else:
            result = clean_pattern.sub('', result)
        version_found = 'Clean'
    
    # Also check for standalone (Dirty) or (Clean)
    if re.search(r'\(dirty\)', result, re.IGNORECASE):
        result = re.sub(r'\s*\(dirty\)', '', result, flags=re.IGNORECASE)
        version_found = 'Dirty'
    if re.search(r'\(clean\)', result, re.IGNORECASE):
        result = re.sub(r'\s*\(clean\)', '', result, flags=re.IGNORECASE)
        version_found = 'Clean'
    
    # Add version at the end if found
    if version_found:
        result = f"{result.strip()} ({version_found})"
    
    # Clean up multiple spaces
    result = re.sub(r'\s+', ' ', result)
    
    return result.strip()


def extract_track_metadata(title):
    """
    Extract metadata from track title.
    Returns dict with: base_title, version (Dirty/Clean), format_type, is_acapella, etc.
    """
    metadata = {
        'original_title': title,
        'base_title': title,
        'version': None,  # Dirty or Clean
        'format_type': None,  # Short, Intro, Outro, Acap In, Acap Out, etc.
        'is_acapella': False,
        'is_acapella_loop': False,
        'is_verse': False,
        'bpm': None,
    }
    
    if not title:
        return metadata
    
    title_lower = title.lower()
    
    # Detect version
    if 'dirty' in title_lower:
        metadata['version'] = 'Dirty'
    elif 'clean' in title_lower:
        metadata['version'] = 'Clean'
    
    # Detect format type
    if 'acap in' in title_lower or 'acapella intro' in title_lower:
        metadata['format_type'] = 'Acap In'
        metadata['is_acapella'] = True
    elif 'acap out' in title_lower or 'acapella outro' in title_lower:
        metadata['format_type'] = 'Acap Out'
        metadata['is_acapella'] = True
    elif 'acapella loop' in title_lower or 'acap loop' in title_lower:
        metadata['format_type'] = 'Acapella Loop'
        metadata['is_acapella'] = True
        metadata['is_acapella_loop'] = True
    elif 'acapella' in title_lower or 'a capella' in title_lower:
        metadata['format_type'] = 'Acapella'
        metadata['is_acapella'] = True
    elif 'short' in title_lower or 'quick hit' in title_lower:
        metadata['format_type'] = 'Short'
    elif 'intro' in title_lower:
        metadata['format_type'] = 'Intro'
    elif 'outro' in title_lower:
        metadata['format_type'] = 'Outro'
    elif 'verse' in title_lower:
        metadata['format_type'] = 'Verse'
        metadata['is_verse'] = True
    
    # Extract BPM if present (e.g., "120 BPM" or "(120)")
    bpm_match = re.search(r'(\d{2,3})\s*bpm', title_lower)
    if bpm_match:
        metadata['bpm'] = int(bpm_match.group(1))
    
    # Fallback: extract BPM from trailing number in title/filename
    # DJ City files have format: 'Artist - Title (Version) BPM.mp3'
    if metadata['bpm'] is None:
        metadata['bpm'] = extract_bpm_from_filename(title)
    
    return metadata


def generate_version_titles(base_title, format_type=None):
    """
    Generate both Dirty and Clean versions of a title.
    Returns list of (title, version) tuples.
    
    Example input: "Best Friend (ID By Rivoli Acap In)"
    Returns: [
        ("Best Friend (ID By Rivoli Acap In) (Dirty)", "Dirty"),
        ("Best Friend (ID By Rivoli Acap In) (Clean)", "Clean")
    ]
    """
    # Remove existing version markers
    clean_base = re.sub(r'\s*\((dirty|clean)\)\s*', '', base_title, flags=re.IGNORECASE).strip()
    
    versions = []
    for version in TRACK_VERSIONS:
        version_title = f"{clean_base} ({version})"
        versions.append((version_title, version))
    
    return versions


def process_track_title_for_import(original_title, original_filename=None):
    """
    Main function to process a track title for import.
    
    Returns dict with:
    - skip: bool - whether to skip this track
    - skip_reason: str - reason for skipping (if skip=True)
    - cleaned_title: str - cleaned title
    - versions: list - list of (title, version) tuples for Dirty/Clean
    - metadata: dict - extracted metadata
    """
    result = {
        'skip': False,
        'skip_reason': None,
        'original_title': original_title,
        'cleaned_title': original_title,
        'versions': [],
        'metadata': {}
    }
    
    # Use filename as fallback
    title_to_process = original_title or original_filename
    if not title_to_process:
        return result
    
    # Check if should skip
    should_skip, reason = should_skip_track(title_to_process)
    if should_skip:
        result['skip'] = True
        result['skip_reason'] = reason
        return result
    
    # Clean the title
    cleaned = clean_track_title(title_to_process)
    result['cleaned_title'] = cleaned
    
    # Extract metadata
    result['metadata'] = extract_track_metadata(cleaned)
    
    # Generate Dirty/Clean versions
    result['versions'] = generate_version_titles(cleaned)
    
    return result


def delete_from_dropbox_if_skipped(dropbox_path, dropbox_token, dropbox_team_member_id=None, namespace_id=None):
    """
    Delete a file from Dropbox (used when track is skipped due to keywords).
    """
    try:
        delete_headers = {
            'Authorization': f'Bearer {dropbox_token}',
            'Content-Type': 'application/json'
        }
        if dropbox_team_member_id:
            delete_headers['Dropbox-API-Select-User'] = dropbox_team_member_id
        if namespace_id:
            delete_headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})
        
        delete_response = requests.post(
            'https://api.dropboxapi.com/2/files/delete_v2',
            headers=delete_headers,
            json={'path': dropbox_path}
        )
        
        return delete_response.status_code == 200
    except Exception as e:
        print(f"⚠️  Error deleting from Dropbox: {e}")
        return False


# =============================================================================
# ID3 TAG MANAGEMENT
# =============================================================================

def update_metadata(filepath, artist, title, original_path, bpm):
    """
    Updates metadata with ONLY the specified fields (clean slate).
    Fields: Title, Artist, Album, Date, Track Number, Genre, BPM, ISRC, Picture, Length, Publisher
    """
    try:
        # Read original file metadata
        try:
            original_audio = MP3(original_path, ID3=ID3)
            original_tags = original_audio.tags
        except:
            original_tags = None
        
        # Clear all existing tags and start fresh
        try:
            audio = MP3(filepath, ID3=ID3)
            audio.delete()  # Remove all tags
            audio.save()
        except:
            pass
        
        # Create new clean ID3 tag
        tags = ID3(filepath)
        
        # Add ONLY specified fields
        
        # 1. Title (from parameter)
        tags.add(TIT2(encoding=3, text=title))
        
        # 2. Artist (from original, formatted with , and &)
        if original_tags and 'TPE1' in original_tags:
            artist_raw = str(original_tags['TPE1'].text[0]) if original_tags['TPE1'].text else ''
            artist_formatted = format_artists(artist_raw)
            tags.add(TPE1(encoding=3, text=artist_formatted))
        
        # 3. Album (from original)
        if original_tags and 'TALB' in original_tags:
            tags.add(TALB(encoding=3, text=original_tags['TALB'].text))
        
        # 4. Date (from original, preserve full format)
        if original_tags and 'TDRC' in original_tags:
            tags.add(TDRC(encoding=3, text=original_tags['TDRC'].text))
        
        # 5. Track Number (from original)
        if original_tags and 'TRCK' in original_tags:
            tags.add(TRCK(encoding=3, text=original_tags['TRCK'].text))
        
        # 6. Genre (from original)
        if original_tags and 'TCON' in original_tags:
            tags.add(TCON(encoding=3, text=original_tags['TCON'].text))
        
        # 7. BPM (from original metadata only, don't auto-detect)
        if bpm is not None:
            tags.add(TBPM(encoding=3, text=str(bpm)))
        
        # 8. ISRC (from original) - IMPORTANT: Always include
        isrc_value = ''
        if original_tags and 'TSRC' in original_tags:
            isrc_value = str(original_tags['TSRC'].text[0]) if original_tags['TSRC'].text else ''
            tags.add(TSRC(encoding=3, text=isrc_value))
        
        # 9. Publisher (keep original as-is) + Label (parent category)
        original_publisher = ''
        print(f"   🔍 DEBUG: original_tags existe? {original_tags is not None}")
        if original_tags:
            print(f"   🔍 DEBUG: Clés tags: {[k for k in original_tags.keys() if k.startswith('T')]}")
            if 'TPUB' in original_tags:
                original_publisher = str(original_tags['TPUB'].text[0]).strip() if original_tags['TPUB'].text else ''
                print(f"   🔍 DEBUG: TPUB trouvé = '{original_publisher}'")
            else:
                print(f"   ⚠️ DEBUG: TPUB absent du fichier original")
        
        print(f"   🔍 DEBUG: original_path = {original_path}")
        print(f"   🔍 DEBUG: Fichier existe? {os.path.exists(original_path)}")
        print(f"   🔍 DEBUG: TPUB lu du fichier original = '{original_publisher}'")
        
        if original_publisher:
            # Keep original publisher in TPUB (unchanged!)
            print(f"   ✅ AJOUT TPUB = '{original_publisher}' (valeur originale)")
            tags.add(TPUB(encoding=3, text=original_publisher))
            
            # Get parent label (Warner/Sony/Universal)
            parent_label = get_parent_label(original_publisher)
            # Only add Label if it's different from publisher (meaning it was mapped)
            if parent_label != original_publisher:
                tags.add(TXXX(encoding=3, desc='LABEL', text=parent_label))
                print(f"   📋 Publisher (TPUB): '{original_publisher}'")
                print(f"   📋 Label (TXXX): '{parent_label}'")
            else:
                print(f"   📋 Publisher (TPUB): '{original_publisher}'")
                print(f"   📋 Label: (non reconnu, pas ajouté)")
        else:
            print(f"   📋 Publisher: (vide dans le fichier original)")
        
        # 10. Custom Track ID: $ISRC_$filename (clean format: no dashes, single underscores only)
        # Extract clean filename (without path and extension)
        filename_base = os.path.splitext(os.path.basename(filepath))[0]
        # Replace dashes with spaces, then normalize spaces, then convert to underscores
        filename_clean = filename_base.replace('-', ' ').replace('_', ' ')
        filename_clean = re.sub(r'\s+', ' ', filename_clean).strip()  # Multiple spaces -> single space
        filename_clean = filename_clean.replace(' ', '_')  # Spaces -> underscores
        filename_clean = re.sub(r'_+', '_', filename_clean)  # Multiple underscores -> single underscore
        
        track_id = f"{isrc_value}_{filename_clean}" if isrc_value else filename_clean
        tags.add(TXXX(encoding=3, desc='TRACK_ID', text=track_id))
        
        # 11. Length
        try:
            audio_info = MP3(filepath)
            length_ms = int(audio_info.info.length * 1000)
            tags.add(TLEN(encoding=3, text=str(length_ms)))
        except:
            pass
        
        # 11. Picture - ID By Rivoli Cover ONLY (no original cover in file)
        cover_path = os.path.join(BASE_DIR, 'assets', 'Cover_Id_by_Rivoli.jpeg')
        if os.path.exists(cover_path):
            with open(cover_path, 'rb') as img:
                tags.add(APIC(
                    encoding=3,
                    mime='image/jpeg',
                    type=3,  # Cover (front) - PRIMARY
                    desc='ID By Rivoli',
                    data=img.read()
                ))
        
        # NOTE: Original cover is NOT added to file - only sent to API via prepare_track_metadata
        
        # URL branding only (no TMED to avoid confusion with Publisher in some players)
        tags.add(WXXX(encoding=3, desc='ID By Rivoli', url='https://www.idbyrivoli.com'))
        
        # Save both ID3v2.3 and ID3v1.1 tags together (preserves all tags including covers)
        tags.save(filepath, v1=2, v2_version=3)  # v1=2 writes ID3v1.1, v2_version=3 writes ID3v2.3
        
        # VERIFICATION: Read back the file to confirm metadata was saved correctly
        verify_audio = MP3(filepath, ID3=ID3)
        verify_tpub = ''
        if verify_audio.tags and 'TPUB' in verify_audio.tags:
            verify_tpub = str(verify_audio.tags['TPUB'].text[0]) if verify_audio.tags['TPUB'].text else ''
        print(f"   ✅ MP3 sauvegardé: {os.path.basename(filepath)}")
        print(f"   🔍 VERIFICATION TPUB dans fichier = '{verify_tpub}'")
        
    except Exception as e:
        print(f"Error updating metadata for {filepath}: {e}")


def update_metadata_wav(filepath, artist, title, original_path, bpm):
    """
    Adds ID3v2 tags to WAV file using mutagen.wave (proper method).
    This embeds ID3 tags correctly without corrupting the WAV structure.
    Same fields as MP3 for consistency.
    """
    try:
        from mutagen.wave import WAVE
        
        # Read original file metadata for reference
        try:
            original_audio = MP3(original_path, ID3=ID3)
            original_tags = original_audio.tags
        except:
            original_tags = None
        
        # Open WAV file and add ID3 tags properly
        audio = WAVE(filepath)
        
        # Add ID3 tag container if not present
        if audio.tags is None:
            audio.add_tags()
        
        # 1. Title (from parameter)
        audio.tags.add(TIT2(encoding=3, text=title))
        
        # 2. Artist (from original, formatted with , and &)
        if original_tags and 'TPE1' in original_tags:
            artist_raw = str(original_tags['TPE1'].text[0]) if original_tags['TPE1'].text else ''
            artist_formatted = format_artists(artist_raw)
            audio.tags.add(TPE1(encoding=3, text=artist_formatted))
        
        # 3. Album (from original)
        if original_tags and 'TALB' in original_tags:
            audio.tags.add(TALB(encoding=3, text=original_tags['TALB'].text))
        
        # 4. Date (from original)
        if original_tags and 'TDRC' in original_tags:
            audio.tags.add(TDRC(encoding=3, text=original_tags['TDRC'].text))
        
        # 5. Track Number (from original)
        if original_tags and 'TRCK' in original_tags:
            audio.tags.add(TRCK(encoding=3, text=original_tags['TRCK'].text))
        
        # 6. Genre (from original)
        if original_tags and 'TCON' in original_tags:
            audio.tags.add(TCON(encoding=3, text=original_tags['TCON'].text))
        
        # 7. BPM (from original metadata only)
        if bpm is not None:
            audio.tags.add(TBPM(encoding=3, text=str(bpm)))
        
        # 8. ISRC (from original)
        isrc_value = ''
        if original_tags and 'TSRC' in original_tags:
            isrc_value = str(original_tags['TSRC'].text[0]) if original_tags['TSRC'].text else ''
            audio.tags.add(TSRC(encoding=3, text=isrc_value))
        
        # 9. Publisher (keep original as-is) + Label (parent category)
        original_publisher = ''
        print(f"   🔍 WAV DEBUG: original_tags existe? {original_tags is not None}")
        if original_tags and 'TPUB' in original_tags:
            original_publisher = str(original_tags['TPUB'].text[0]).strip() if original_tags['TPUB'].text else ''
            print(f"   🔍 WAV DEBUG: TPUB original = '{original_publisher}'")
        else:
            print(f"   ⚠️ WAV DEBUG: TPUB absent du fichier original")
        
        if original_publisher:
            # Keep original publisher in TPUB
            print(f"   ✅ WAV AJOUT TPUB = '{original_publisher}'")
            audio.tags.add(TPUB(encoding=3, text=original_publisher))
            
            # Get parent label (Warner/Sony/Universal)
            parent_label = get_parent_label(original_publisher)
            # Only add Label if it's different from publisher (meaning it was mapped)
            if parent_label != original_publisher:
                print(f"   ✅ WAV AJOUT LABEL = '{parent_label}'")
                audio.tags.add(TXXX(encoding=3, desc='LABEL', text=parent_label))
        
        # 10. Custom Track ID
        filename_base = os.path.splitext(os.path.basename(filepath))[0]
        filename_clean = filename_base.replace('-', ' ').replace('_', ' ')
        filename_clean = re.sub(r'\s+', ' ', filename_clean).strip()
        filename_clean = filename_clean.replace(' ', '_')
        filename_clean = re.sub(r'_+', '_', filename_clean)
        track_id = f"{isrc_value}_{filename_clean}" if isrc_value else filename_clean
        audio.tags.add(TXXX(encoding=3, desc='TRACK_ID', text=track_id))
        
        # 11. Picture - ID By Rivoli Cover as PRIMARY (type=3)
        cover_path = os.path.join(BASE_DIR, 'assets', 'Cover_Id_by_Rivoli.jpeg')
        if os.path.exists(cover_path):
            with open(cover_path, 'rb') as img:
                audio.tags.add(APIC(
                    encoding=3,
                    mime='image/jpeg',
                    type=3,  # Cover (front) - PRIMARY
                    desc='ID By Rivoli',
                    data=img.read()
                ))
        
        # NOTE: Original cover is NOT added to file - only sent to API via prepare_track_metadata
        
        # URL branding only (no TMED to avoid confusion with Publisher in some players)
        audio.tags.add(WXXX(encoding=3, desc='ID By Rivoli', url='https://www.idbyrivoli.com'))
        
        # Save properly embedded in WAV structure
        audio.save()
        
        # VERIFICATION: Read back the file to confirm metadata was saved correctly
        from mutagen.wave import WAVE as WAVE_VERIFY
        verify_audio = WAVE_VERIFY(filepath)
        verify_tpub = ''
        if verify_audio.tags and 'TPUB' in verify_audio.tags:
            verify_tpub = str(verify_audio.tags['TPUB'].text[0]) if verify_audio.tags['TPUB'].text else ''
        print(f"   ✅ WAV sauvegardé: {os.path.basename(filepath)}")
        print(f"   🔍 WAV VERIFICATION TPUB = '{verify_tpub}'")
        
    except Exception as e:
        print(f"   ⚠️ WAV metadata error: {e}")


# =============================================================================
# API / DATABASE EXPORT
# =============================================================================

def prepare_track_metadata(edit_info, original_path, bpm, base_url="", allow_no_deezer=False):
    """
    Prepares track metadata for API export with absolute URLs.
    If allow_no_deezer=True (e.g. deemix upload-only), sends metadata even without a Deezer match.
    """
    import config as _cfg
    
    # Use mutable CURRENT_HOST_URL from config
    base_url = _cfg.CURRENT_HOST_URL if _cfg.CURRENT_HOST_URL else ""
    
    # Warn if we don't have a valid public URL
    if not base_url or 'localhost' in base_url:
        print(f"⚠️ WARNING: No valid public URL detected! API calls may fail.")
        print(f"   Current CURRENT_HOST_URL: {_cfg.CURRENT_HOST_URL}")
        print(f"   Set PUBLIC_URL env variable or access the app via its public URL first.")
    
    try:
        # Read original metadata
        original_audio = MP3(original_path, ID3=ID3)
        original_tags = original_audio.tags if original_audio.tags else {}
        
        # Extract fields
        artist_raw = str(original_tags.get('TPE1', 'Unknown')).strip() if 'TPE1' in original_tags else 'Unknown'
        artist = format_artists(artist_raw)  # Format multiple artists with , and &
        album = str(original_tags.get('TALB', '')).strip() if 'TALB' in original_tags else ''
        genre = str(original_tags.get('TCON', '')).strip() if 'TCON' in original_tags else ''
        
        # ISRC extraction
        isrc = ''
        if 'TSRC' in original_tags:
            isrc = str(original_tags['TSRC'].text[0]).strip() if original_tags['TSRC'].text else ''
        
        # Date handling
        date_str = str(original_tags.get('TDRC', '')).strip() if 'TDRC' in original_tags else ''
        try:
            if date_str:
                date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                date_sortie = int(date_obj.timestamp())
            else:
                date_sortie = 0
        except:
            date_sortie = 0
        
        # Publisher/Label: original goes to Sous-label, mapped goes to Label
        sous_label = ''  # Original publisher from file
        parent_label = ''  # Mapped parent label
        if 'TPUB' in original_tags and original_tags['TPUB'].text:
            sous_label = str(original_tags['TPUB'].text[0]).strip()
            # Map sub-label to parent label
            parent_label = get_parent_label(sous_label) if sous_label else ''
            # If mapping didn't change it (not in our list), parent_label = sous_label
            # In that case, we don't have a parent, so leave Label empty
            if parent_label == sous_label:
                parent_label = ''  # No known parent for this sub-label
        
        # Construct ABSOLUTE URLs using DYNAMIC BASE URL
        relative_url = edit_info.get('url', '')
        absolute_url = f"{base_url}{relative_url}" if relative_url else ''
        
        # Extract original cover ONLY for deemix tracks (they have correct Deezer covers)
        # For all other sources (DJ pools, etc.), original covers are branded/wrong — skip them
        cover_url = ''  # Will be filled by Deezer API if match is good
        original_cover_found = False
        # Check if source is deemix: file path, or Dropbox bulk import folder
        is_from_deemix = 'deemix' in original_path.lower()
        if not is_from_deemix and bulk_import_state.get('active'):
            is_from_deemix = 'deemix' in bulk_import_state.get('folder_path', '').lower()
        
        if is_from_deemix and original_tags:
            print(f"   📂 Deemix source detected — extracting original cover")
            # Look for any APIC (cover art) that is NOT the ID By Rivoli cover
            for apic_key in original_tags.keys():
                if apic_key.startswith('APIC'):
                    try:
                        original_apic = original_tags[apic_key]
                        
                        # Skip if this is our ID By Rivoli cover (check description)
                        apic_desc = getattr(original_apic, 'desc', '')
                        if 'ID By Rivoli' in str(apic_desc):
                            print(f"   ⏭️ Skipping ID By Rivoli cover: {apic_key}")
                            continue
                        
                        # Generate unique filename based on track
                        track_name_clean = re.sub(r'[^\w\s-]', '', os.path.splitext(os.path.basename(original_path))[0])
                        track_name_clean = track_name_clean.replace(' ', '_')[:50]
                        
                        # Determine extension from mime type
                        mime = getattr(original_apic, 'mime', 'image/jpeg')
                        ext = 'jpg' if 'jpeg' in mime.lower() else 'png'
                        cover_filename = f"cover_{track_name_clean}.{ext}"
                        cover_save_path = os.path.join(BASE_DIR, 'static', 'covers', cover_filename)
                        
                        # Save the original cover
                        with open(cover_save_path, 'wb') as f:
                            f.write(original_apic.data)
                        
                        # Use the original cover URL
                        cover_url = f"{base_url}/static/covers/{cover_filename}"
                        original_cover_found = True
                        print(f"   ✅ Cover originale extraite (deemix): {cover_filename}")
                        break
                    except Exception as e:
                        print(f"   ❌ Could not extract cover from {apic_key}: {e}")
        elif not is_from_deemix:
            print(f"   🚫 Non-deemix source — skipping original cover extraction")
        
        # ─── Deezer API: enrich metadata (ISRC, BPM, album, label, genre, cover) ───
        search_artist = artist if artist != 'Unknown' else ''
        # Use the ORIGINAL track title (TIT2 tag) for Deezer search, NOT edit_info['name']
        # edit_info['name'] is "BaseName - Variant" (e.g., "ParoVie (feat. Damso) - Main")
        # which clean_detected_type_from_title splits on " - " and reduces to just the
        # variant suffix ("Main", "Acapella", "Instrumental") instead of the real title.
        original_title_tag = str(original_tags.get('TIT2', '')).strip() if 'TIT2' in original_tags else ''
        if original_title_tag:
            search_title = clean_detected_type_from_title(original_title_tag)
        else:
            # Fallback: original filename without extension
            search_title = clean_detected_type_from_title(os.path.splitext(os.path.basename(original_path))[0])
        
        deezer_meta = {}
        try:
            print(f"   🔍 Searching Deezer: '{search_artist}' - '{search_title}'")
            deezer_meta = search_deezer_metadata(search_artist, search_title)
            if deezer_meta.get('deezer_id'):
                print(f"   ✅ Deezer match (score {deezer_meta.get('match_score', 0.0):.2f}): {deezer_meta.get('artist')} - {deezer_meta.get('title')} (ISRC: {deezer_meta.get('isrc', 'N/A')})")
                
                # Fill missing fields from Deezer (ID3 tags take priority)
                if not isrc and deezer_meta.get('isrc'):
                    isrc = deezer_meta['isrc']
                    print(f"   📝 ISRC from Deezer: {isrc}")
                if (bpm is None or bpm == 0) and deezer_meta.get('bpm'):
                    bpm = deezer_meta['bpm']
                    print(f"   📝 BPM from Deezer: {bpm}")
                if not album and deezer_meta.get('album'):
                    album = deezer_meta['album']
                    print(f"   📝 Album from Deezer: {album}")
                if not genre and deezer_meta.get('genre'):
                    genre = deezer_meta['genre']
                    print(f"   📝 Genre from Deezer: {genre}")
                if not sous_label and deezer_meta.get('label'):
                    sous_label = deezer_meta['label']
                    parent_label = get_parent_label(sous_label) if sous_label else ''
                    if parent_label == sous_label:
                        parent_label = ''
                    print(f"   📝 Label from Deezer: {sous_label}")
                if not date_sortie and deezer_meta.get('release_date'):
                    try:
                        date_obj = datetime.strptime(deezer_meta['release_date'][:10], '%Y-%m-%d')
                        date_sortie = int(date_obj.timestamp())
                        print(f"   📝 Release date from Deezer: {deezer_meta['release_date']}")
                    except:
                        pass
                
                # Cover from Deezer
                if deezer_meta.get('cover_url'):
                    cover_url = deezer_meta['cover_url']
                    print(f"   🖼️ Cover from Deezer: {cover_url[:80]}...")
            else:
                print(f"   ⚠️ No Deezer match found")
        except Exception as e:
            print(f"   ⚠️ Deezer lookup failed: {e}")
        
        # ─── FILTER: Skip if no Deezer match (unless allow_no_deezer for deemix upload-only) ───
        if not deezer_meta.get('deezer_id') and not allow_no_deezer:
            print(f"   ⏭️ SKIPPED: No confident Deezer match - track not written to DB")
            return None
        if not deezer_meta.get('deezer_id') and allow_no_deezer:
            print(f"   📤 Deemix upload-only: sending with file metadata only (no Deezer match)")
        
        # Generate Track ID (clean format: no dashes, single underscores only)
        filename_raw = edit_info.get('name', '')
        filename_clean = filename_raw.replace('-', ' ').replace('_', ' ')
        filename_clean = re.sub(r'\s+', ' ', filename_clean).strip()
        filename_clean = filename_clean.replace(' ', '_')
        filename_clean = re.sub(r'_+', '_', filename_clean)
        
        track_id = f"{isrc}_{filename_clean}" if isrc else filename_clean
        
        # Prepare data structure
        # Label = parent label (Universal, Sony, Warner, Alfred) if sub-label is recognized
        # Sous-label = original publisher from file
        track_data = {
            'Type': edit_info.get('type', ''),
            'Format': edit_info.get('format', 'MP3'),
            'Titre': edit_info.get('name', ''),
            'Artiste': artist,
            'Fichiers': absolute_url,
            'Univers': '',
            'Mood': '',
            'Style': genre,
            'Album': album,
            'Label': parent_label,  # Mapped parent label (or empty if not recognized)
            'Sous-label': sous_label,  # Original publisher from file
            'Date de sortie': date_sortie,
            'BPM': bpm if bpm is not None else 0,
            'Artiste original': artist,
            'Url': cover_url,
            'ISRC': isrc,
            'TRACK_ID': track_id,
            '_force_cover_replace': bool(deezer_meta.get('cover_url')),
        }
        
        return track_data
        
    except Exception as e:
        print(f"Error preparing track metadata: {e}")
        return None


def send_track_info_to_api(track_data):
    """
    Sends track information to external API endpoint with authentication,
    OR saves directly to database if USE_DATABASE_MODE is enabled.
    """
    import config as _cfg

    # Log the payload being processed
    print(f"\n{'='*60}")
    print(f"📤 TRACK DATA for: {track_data.get('Titre', 'N/A')} ({track_data.get('Format', 'N/A')})")
    print(f"{'='*60}")
    print(json.dumps(track_data, indent=2, ensure_ascii=False))
    print(f"{'='*60}\n")
    
    # Use database mode if enabled
    if _cfg.USE_DATABASE_MODE:
        try:
            from database_service import save_track_to_database
            result = save_track_to_database(track_data)
            
            if 'error' in result:
                print(f"❌ DATABASE ERROR: {result['error']}")
                _log_message(f"DB ERROR: {track_data['Titre']} - {result['error']}")
            else:
                action = result.get('action', 'saved')
                print(f"✅ DATABASE SUCCESS: {track_data['Titre']} ({track_data['Format']}) [{action}]")
                _log_message(f"DB OK: {track_data['Titre']} ({track_data['Format']}) → {result.get('id', 'N/A')} [{action}]")
            
            return result
            
        except ImportError:
            print(f"❌ DATABASE SERVICE NOT AVAILABLE - falling back to API")
        except Exception as e:
            print(f"❌ DATABASE EXCEPTION: {e}")
            _log_message(f"DB EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    # Fall back to API mode
    if not _cfg.API_ENDPOINT:
        print("⚠️  API_ENDPOINT not configured, skipping API call")
        return
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {_cfg.API_KEY}'
        }
        
        response = requests.post(_cfg.API_ENDPOINT, json=track_data, headers=headers, timeout=30)
        
        if response.status_code in [200, 202]:
            print(f"✅ API SUCCESS: {track_data['Titre']} ({track_data['Format']})")
            _log_message(f"API OK: {track_data['Titre']} ({track_data['Format']}) → {track_data.get('Fichiers', '')}")
        else:
            print(f"❌ API ERROR {response.status_code}: {response.text[:200]}")
            _log_message(f"API ERROR {response.status_code} pour {track_data['Titre']}")
            
    except Exception as e:
        print(f"❌ API EXCEPTION: {e}")
        _log_message(f"API EXCEPTION: {e}")
