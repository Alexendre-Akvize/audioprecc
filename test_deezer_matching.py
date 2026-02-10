#!/usr/bin/env python3
"""
Test Deezer matching for all tracks on Dropbox.
Scans Dropbox recursively, extracts artist/title from filenames,
runs the Deezer matching logic, and outputs a CSV with results.

Usage:
    python test_deezer_matching.py                          # Scan entire Dropbox
    python test_deezer_matching.py --folder /deemix          # Scan specific folder
    python test_deezer_matching.py --folder /bpm_supreme     # Scan specific folder
    python test_deezer_matching.py --limit 50                # Only test first 50 tracks

Output: deezer_match_results.csv
"""

import os
import re
import sys
import csv
import json
import time
import unicodedata
import requests
from datetime import datetime

# ─── Load env from .env file ───
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, '.env')
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ.setdefault(key, value)

# ─── Dropbox config ───
DROPBOX_REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', '')
DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')
DROPBOX_APP_SECRET = os.environ.get('DROPBOX_APP_SECRET', '')
DROPBOX_ACCESS_TOKEN = os.environ.get('DROPBOX_ACCESS_TOKEN', '')
DROPBOX_TEAM_MEMBER_ID = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '')

# ─── Skip keywords (from app.py) ───
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


# =============================================================================
# DROPBOX AUTH
# =============================================================================

def get_dropbox_token():
    """Get a valid Dropbox access token, refreshing if needed."""
    if DROPBOX_REFRESH_TOKEN and DROPBOX_APP_KEY and DROPBOX_APP_SECRET:
        try:
            response = requests.post(
                'https://api.dropbox.com/oauth2/token',
                data={'grant_type': 'refresh_token', 'refresh_token': DROPBOX_REFRESH_TOKEN},
                auth=(DROPBOX_APP_KEY, DROPBOX_APP_SECRET)
            )
            if response.status_code == 200:
                return response.json().get('access_token', '')
            else:
                print(f"❌ Token refresh failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Token refresh error: {e}")
    
    if DROPBOX_ACCESS_TOKEN:
        return DROPBOX_ACCESS_TOKEN
    
    print("❌ No Dropbox credentials found. Set DROPBOX_REFRESH_TOKEN, DROPBOX_APP_KEY, DROPBOX_APP_SECRET in .env")
    sys.exit(1)


# =============================================================================
# DROPBOX SCAN
# =============================================================================

def scan_dropbox_folder(token, folder_path=''):
    """Recursively scan a Dropbox folder for audio files."""
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    if DROPBOX_TEAM_MEMBER_ID:
        headers['Dropbox-API-Select-User'] = DROPBOX_TEAM_MEMBER_ID
        
        # Auto-detect namespace
        try:
            account_response = requests.post(
                'https://api.dropboxapi.com/2/users/get_current_account',
                headers={
                    'Authorization': f'Bearer {token}',
                    'Dropbox-API-Select-User': DROPBOX_TEAM_MEMBER_ID
                }
            )
            if account_response.status_code == 200:
                root_info = account_response.json().get('root_info', {})
                namespace_id = root_info.get('root_namespace_id', '')
                if namespace_id:
                    headers['Dropbox-API-Path-Root'] = json.dumps({
                        '.tag': 'namespace_id', 'namespace_id': namespace_id
                    })
                    print(f"📦 Using namespace: {namespace_id}")
        except Exception as e:
            print(f"⚠️ Namespace detection error: {e}")
    
    all_files = []
    has_more = True
    cursor = None
    
    print(f"🔍 Scanning '{folder_path or '(root)'}' recursively...")
    
    while has_more:
        if cursor:
            response = requests.post(
                'https://api.dropboxapi.com/2/files/list_folder/continue',
                headers=headers,
                json={'cursor': cursor}
            )
        else:
            response = requests.post(
                'https://api.dropboxapi.com/2/files/list_folder',
                headers=headers,
                json={
                    'path': folder_path,
                    'recursive': True,
                    'include_media_info': False,
                    'include_deleted': False,
                    'limit': 2000
                }
            )
        
        if response.status_code != 200:
            error_msg = 'Unknown error'
            try:
                error_msg = response.json().get('error_summary', error_msg)
            except:
                pass
            print(f"❌ Dropbox API error: {response.status_code} - {error_msg}")
            break
        
        result = response.json()
        
        for entry in result.get('entries', []):
            if entry.get('.tag') == 'file':
                name = entry.get('name', '').lower()
                if name.endswith('.mp3') or name.endswith('.wav'):
                    all_files.append({
                        'name': entry.get('name'),
                        'path': entry.get('path_display'),
                        'size': entry.get('size', 0),
                    })
        
        has_more = result.get('has_more', False)
        cursor = result.get('cursor')
        print(f"   Found {len(all_files)} audio files so far...")
    
    print(f"✅ Scan complete: {len(all_files)} audio files found\n")
    return all_files


# =============================================================================
# TITLE/ARTIST EXTRACTION FROM FILENAME
# =============================================================================

def extract_artist_title(filename):
    """Extract artist and title from a filename like 'Artist - Title (Version) BPM.mp3'."""
    name = os.path.splitext(filename)[0]
    
    # Remove trailing BPM number (e.g., "Artist - Title 128")
    name = re.sub(r'\s+\d{2,3}\s*$', '', name)
    
    # Remove trailing Camelot key + BPM (e.g., "10A 93", "1B")
    name = re.sub(r'\s+\d{1,2}[AB]\s+\d{2,3}\s*$', '', name)
    name = re.sub(r'\s+\d{1,2}[AB]\s*$', '', name)
    
    artist = 'Unknown'
    title = name
    
    if ' - ' in name:
        parts = name.split(' - ', 1)
        artist = parts[0].strip()
        title = parts[1].strip()
    
    return artist, title


def clean_title_for_search(title):
    """Clean title for Deezer search (remove version markers, parentheticals, etc.)."""
    # Remove version markers in parentheses
    cleaned = re.sub(r'\s*\([^)]*(?:clean|dirty|intro|outro|acap|instrumental|inst|short|quick hit|extended|main|original)[^)]*\)', '', title, flags=re.IGNORECASE)
    # Remove trailing version markers after dash
    cleaned = re.sub(r'\s*-\s*(?:clean|dirty|intro|outro|acapella|instrumental|inst|short|quick hit|extended|main|original)\s*$', '', cleaned, flags=re.IGNORECASE)
    # Clean up
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned or title


def should_skip(title):
    """Check if title contains skip keywords."""
    title_lower = title.lower()
    for kw in SKIP_KEYWORDS:
        if kw in title_lower:
            return True, kw
    return False, None


# =============================================================================
# DEEZER MATCHING (same logic as app.py with scoring)
# =============================================================================

def _normalize(s):
    """Lowercase, strip accents, remove punctuation for comparison."""
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
    if nq and nc and (nq in nc or nc in nq):
        return 1.0
    return 0.0


def _score_track(t, searched_artist, searched_title):
    """Score a Deezer result (0-1) against searched artist+title."""
    t_artist = t.get('artist', {}).get('name', '')
    t_title = t.get('title', '')

    artist_score = max(
        _word_overlap_score(searched_artist, t_artist),
        _contains_score(searched_artist, t_artist)
    )
    title_score = max(
        _word_overlap_score(searched_title, t_title),
        _contains_score(searched_title, t_title)
    )
    return artist_score * 0.4 + title_score * 0.6


def search_deezer(artist, title, timeout=10):
    """Search Deezer API with scoring logic matching app.py."""
    MINIMUM_MATCH_SCORE = 0.50
    
    clean_artist = re.sub(r'\s*(feat\.?|ft\.?|featuring)\s+.*$', '', artist, flags=re.IGNORECASE).strip()
    clean_title = re.sub(r'\s*\(.*?\)', '', title).strip()
    clean_title = re.sub(r'\s*-\s*$', '', clean_title).strip()
    
    if not clean_artist or not clean_title:
        return None
    
    try:
        search_url = 'https://api.deezer.com/search'
        params = {'q': f'artist:"{clean_artist}" track:"{clean_title}"', 'limit': 5}
        resp = requests.get(search_url, params=params, timeout=timeout)
        
        if resp.status_code != 200:
            return None
        
        tracks = resp.json().get('data', [])
        
        if not tracks:
            # Fallback: simpler search
            params = {'q': f'{clean_artist} {clean_title}', 'limit': 5}
            resp = requests.get(search_url, params=params, timeout=timeout)
            if resp.status_code == 200:
                tracks = resp.json().get('data', [])
        
        if not tracks:
            return None
        
        # Score all candidates
        best_track = None
        best_score = -1.0
        all_candidates = []
        
        for t in tracks:
            s = _score_track(t, clean_artist, clean_title)
            t_artist = t.get('artist', {}).get('name', '')
            t_title = t.get('title', '')
            all_candidates.append({
                'artist': t_artist,
                'title': t_title,
                'score': s,
                'album': t.get('album', {}).get('title', ''),
            })
            if s > best_score:
                best_score = s
                best_track = t
        
        if best_score < MINIMUM_MATCH_SCORE or best_track is None:
            return {
                'matched': False,
                'best_score': best_score,
                'searched_artist': clean_artist,
                'searched_title': clean_title,
                'candidates': all_candidates,
            }
        
        # Get full details
        deezer_id = best_track.get('id')
        result = {
            'matched': True,
            'best_score': best_score,
            'searched_artist': clean_artist,
            'searched_title': clean_title,
            'deezer_id': deezer_id,
            'deezer_artist': best_track.get('artist', {}).get('name', ''),
            'deezer_title': best_track.get('title', ''),
            'deezer_album': best_track.get('album', {}).get('title', ''),
            'cover_url': best_track.get('album', {}).get('cover_xl', ''),
            'candidates': all_candidates,
        }
        
        # Get ISRC from full details
        if deezer_id:
            try:
                detail_resp = requests.get(f'https://api.deezer.com/track/{deezer_id}', timeout=timeout)
                if detail_resp.status_code == 200:
                    detail = detail_resp.json()
                    result['isrc'] = detail.get('isrc', '')
                    result['bpm'] = detail.get('bpm') if detail.get('bpm') and detail.get('bpm') > 0 else None
                    album_data = detail.get('album', {})
                    if album_data:
                        result['label'] = album_data.get('label', '')
                        result['release_date'] = album_data.get('release_date', '')
            except:
                pass
        
        return result
    
    except Exception as e:
        return {'matched': False, 'error': str(e), 'searched_artist': clean_artist, 'searched_title': clean_title}


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Deezer matching for Dropbox tracks')
    parser.add_argument('--folder', type=str, default='', help='Dropbox folder path (e.g., /deemix)')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of tracks to test (0 = all)')
    parser.add_argument('--output', type=str, default='deezer_match_results.csv', help='Output CSV file')
    parser.add_argument('--delay', type=float, default=0.3, help='Delay between Deezer API calls (seconds)')
    args = parser.parse_args()
    
    # Get Dropbox token
    print("🔑 Authenticating with Dropbox...")
    token = get_dropbox_token()
    print("✅ Dropbox authenticated\n")
    
    # Scan folder
    folder_path = args.folder.strip()
    if folder_path and not folder_path.startswith('/'):
        folder_path = '/' + folder_path
    if folder_path == '/':
        folder_path = ''
    
    files = scan_dropbox_folder(token, folder_path)
    
    if args.limit > 0:
        files = files[:args.limit]
        print(f"📝 Limited to {args.limit} tracks\n")
    
    if not files:
        print("❌ No audio files found")
        return
    
    # Prepare CSV
    output_path = os.path.join(BASE_DIR, args.output)
    csv_fields = [
        'origin_folder', 'dropbox_path', 'filename', 'format',
        'extracted_artist', 'extracted_title', 'search_artist', 'search_title',
        'skipped', 'skip_reason',
        'deezer_matched', 'match_score',
        'deezer_artist', 'deezer_title', 'deezer_album',
        'isrc', 'bpm', 'label', 'release_date',
        'cover_url',
        'candidate_1', 'candidate_1_score',
        'candidate_2', 'candidate_2_score',
        'candidate_3', 'candidate_3_score',
        'error',
    ]
    
    # Stats
    total = len(files)
    matched = 0
    not_matched = 0
    skipped = 0
    errors = 0
    
    print(f"{'='*70}")
    print(f"  Testing Deezer matching for {total} tracks")
    print(f"  Output: {output_path}")
    print(f"{'='*70}\n")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
        writer.writeheader()
        
        for i, file_info in enumerate(files):
            filename = file_info['name']
            dropbox_path = file_info['path']
            
            # Extract origin folder (first directory after root)
            path_parts = dropbox_path.strip('/').split('/')
            origin_folder = path_parts[0] if len(path_parts) > 1 else '(root)'
            
            # Format
            file_format = 'WAV' if filename.lower().endswith('.wav') else 'MP3'
            
            # Extract artist/title
            artist, title = extract_artist_title(filename)
            search_title = clean_title_for_search(title)
            
            # Initialize row
            row = {
                'origin_folder': origin_folder,
                'dropbox_path': dropbox_path,
                'filename': filename,
                'format': file_format,
                'extracted_artist': artist,
                'extracted_title': title,
                'search_artist': re.sub(r'\s*(feat\.?|ft\.?|featuring)\s+.*$', '', artist, flags=re.IGNORECASE).strip(),
                'search_title': search_title,
                'skipped': '',
                'skip_reason': '',
                'deezer_matched': '',
                'match_score': '',
                'deezer_artist': '',
                'deezer_title': '',
                'deezer_album': '',
                'isrc': '',
                'bpm': '',
                'label': '',
                'release_date': '',
                'cover_url': '',
                'candidate_1': '', 'candidate_1_score': '',
                'candidate_2': '', 'candidate_2_score': '',
                'candidate_3': '', 'candidate_3_score': '',
                'error': '',
            }
            
            # Check skip keywords
            should_skip_track, skip_kw = should_skip(title)
            if should_skip_track:
                row['skipped'] = 'YES'
                row['skip_reason'] = skip_kw
                skipped += 1
                writer.writerow(row)
                progress = f"[{i+1}/{total}] ⏭️  SKIP ({skip_kw}): {filename}"
                print(progress)
                continue
            
            # Search Deezer
            if artist == 'Unknown':
                row['skipped'] = 'YES'
                row['skip_reason'] = 'No artist in filename'
                skipped += 1
                writer.writerow(row)
                print(f"[{i+1}/{total}] ⏭️  NO ARTIST: {filename}")
                continue
            
            result = search_deezer(artist, search_title)
            
            if result is None:
                row['deezer_matched'] = 'NO'
                row['error'] = 'No results from API'
                not_matched += 1
                writer.writerow(row)
                print(f"[{i+1}/{total}] ❌ NO RESULT: {artist} - {search_title}")
            elif result.get('error'):
                row['deezer_matched'] = 'ERROR'
                row['error'] = result['error']
                errors += 1
                writer.writerow(row)
                print(f"[{i+1}/{total}] ⚠️  ERROR: {filename} - {result['error']}")
            elif result.get('matched'):
                row['deezer_matched'] = 'YES'
                row['match_score'] = f"{result['best_score']:.2f}"
                row['deezer_artist'] = result.get('deezer_artist', '')
                row['deezer_title'] = result.get('deezer_title', '')
                row['deezer_album'] = result.get('deezer_album', '')
                row['isrc'] = result.get('isrc', '')
                row['bpm'] = result.get('bpm', '') or ''
                row['label'] = result.get('label', '')
                row['release_date'] = result.get('release_date', '')
                row['cover_url'] = result.get('cover_url', '')
                
                # Add candidates
                for idx, cand in enumerate(result.get('candidates', [])[:3]):
                    row[f'candidate_{idx+1}'] = f"{cand['artist']} - {cand['title']}"
                    row[f'candidate_{idx+1}_score'] = f"{cand['score']:.2f}"
                
                matched += 1
                writer.writerow(row)
                score_str = f"{result['best_score']:.2f}"
                print(f"[{i+1}/{total}] ✅ {score_str} | {artist} - {search_title} → {result['deezer_artist']} - {result['deezer_title']}")
            else:
                row['deezer_matched'] = 'NO'
                row['match_score'] = f"{result.get('best_score', 0):.2f}"
                
                # Add candidates even for non-matches
                for idx, cand in enumerate(result.get('candidates', [])[:3]):
                    row[f'candidate_{idx+1}'] = f"{cand['artist']} - {cand['title']}"
                    row[f'candidate_{idx+1}_score'] = f"{cand['score']:.2f}"
                
                not_matched += 1
                writer.writerow(row)
                score_str = f"{result.get('best_score', 0):.2f}"
                print(f"[{i+1}/{total}] ❌ {score_str} | {artist} - {search_title} (best below 0.50)")
            
            # Rate limit: Deezer free API ~50 req/5s
            time.sleep(args.delay)
    
    # Summary
    print(f"\n{'='*70}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*70}")
    print(f"  Total tracks:    {total}")
    print(f"  ✅ Matched:       {matched} ({matched/total*100:.1f}%)")
    print(f"  ❌ Not matched:   {not_matched} ({not_matched/total*100:.1f}%)")
    print(f"  ⏭️  Skipped:       {skipped} ({skipped/total*100:.1f}%)")
    print(f"  ⚠️  Errors:        {errors} ({errors/total*100:.1f}%)")
    print(f"\n  📄 Results saved to: {output_path}")
    print(f"{'='*70}\n")


if __name__ == '__main__':
    main()
