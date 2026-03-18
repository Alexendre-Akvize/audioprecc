#!/usr/bin/env python3
"""
Reprocess a single track from Dropbox through the full pipeline:
  1. Locate the file in Dropbox (searches /track done/ and root)
  2. Get a temporary Dropbox download link (for S3 upload)
  3. Download the file locally to read ID3 tags
  4. Run Deezer enrichment (same as live pipeline)
  5. Push track_data to send_track_info_to_api → S3 (ISRC-prefixed) + DB update

Usage:
    python3 reprocess_track.py "Adele - Hello (Intro Clean) 78.mp3"
    python3 reprocess_track.py "Adele - Hello (Intro Clean) 78.mp3" --dry-run
"""

import os
import sys
import json
import re
import tempfile
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from config import DROPBOX_TEAM_MEMBER_ID
from services.dropbox_service import get_valid_dropbox_token


# ─── Dropbox helpers ──────────────────────────────────────────────────────────

def get_dropbox_headers(token, team_id, namespace_id=''):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }
    if team_id:
        headers['Dropbox-API-Select-User'] = team_id
    if namespace_id:
        headers['Dropbox-API-Path-Root'] = json.dumps(
            {'.tag': 'namespace_id', 'namespace_id': namespace_id}
        )
    return headers


def get_namespace(token, team_id):
    try:
        r = requests.post(
            'https://api.dropboxapi.com/2/users/get_current_account',
            headers={'Authorization': f'Bearer {token}',
                     'Dropbox-API-Select-User': team_id},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json().get('root_info', {}).get('root_namespace_id', '')
    except Exception as e:
        print(f'⚠️  Namespace detection error: {e}')
    return ''


def search_dropbox(filename, token, team_id, namespace_id):
    """Search for file in Dropbox, checking /track done/ first then root."""
    headers = get_dropbox_headers(token, team_id, namespace_id)

    # Try /track done/ first
    candidates = [
        f'/track done/{filename}',
        f'/{filename}',
    ]
    for path in candidates:
        check_headers = {**headers, 'Content-Type': 'application/json'}
        r = requests.post(
            'https://api.dropboxapi.com/2/files/get_metadata',
            headers=check_headers,
            json={'path': path},
            timeout=15,
        )
        if r.status_code == 200:
            print(f'✅ Found in Dropbox: {path}')
            return path

    # Use search API as fallback
    print(f'🔍 Searching Dropbox for: {filename}')
    r = requests.post(
        'https://api.dropboxapi.com/2/files/search_v2',
        headers=headers,
        json={
            'query': filename,
            'options': {'max_results': 10, 'file_status': 'active', 'filename_only': True},
        },
        timeout=30,
    )
    if r.status_code == 200:
        for m in r.json().get('matches', []):
            meta = m.get('metadata', {}).get('metadata', {})
            if meta.get('.tag') == 'file' and meta.get('name', '').lower() == filename.lower():
                path = meta.get('path_display', '')
                print(f'✅ Found via search: {path}')
                return path
    print(f'❌ File not found in Dropbox: {filename}')
    return None


def get_temp_link(dropbox_path, token, team_id, namespace_id):
    """Get a temporary direct download URL from Dropbox."""
    headers = get_dropbox_headers(token, team_id, namespace_id)
    r = requests.post(
        'https://api.dropboxapi.com/2/files/get_temporary_link',
        headers=headers,
        json={'path': dropbox_path},
        timeout=15,
    )
    if r.status_code == 200:
        link = r.json().get('link', '')
        print(f'🔗 Temporary link obtained ({len(link)} chars)')
        return link
    print(f'❌ Failed to get temporary link: {r.status_code} {r.text[:200]}')
    return None


def download_file_locally(dropbox_path, token, team_id, namespace_id):
    """Download Dropbox file to a temp local path, return (local_path, filename)."""
    headers = {
        'Authorization': f'Bearer {token}',
        'Dropbox-API-Arg': json.dumps({'path': dropbox_path}),
    }
    if team_id:
        headers['Dropbox-API-Select-User'] = team_id
    if namespace_id:
        headers['Dropbox-API-Path-Root'] = json.dumps(
            {'.tag': 'namespace_id', 'namespace_id': namespace_id}
        )

    print(f'📥 Downloading: {dropbox_path}')
    r = requests.post(
        'https://content.dropboxapi.com/2/files/download',
        headers=headers,
        stream=True,
        timeout=300,
    )
    if r.status_code != 200:
        print(f'❌ Download failed: {r.status_code}')
        return None, None

    filename = os.path.basename(dropbox_path)
    ext = os.path.splitext(filename)[1] or '.mp3'
    tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
    for chunk in r.iter_content(chunk_size=65536):
        if chunk:
            tmp.write(chunk)
    tmp.flush()
    tmp.close()
    size_mb = os.path.getsize(tmp.name) / 1024 / 1024
    print(f'✅ Downloaded: {size_mb:.1f} MB → {tmp.name}')
    return tmp.name, filename


# ─── Full pipeline ────────────────────────────────────────────────────────────

def detect_track_type(filename, local_path):
    """Detect track type from filename/metadata (Intro Clean, Intro, Main, etc.)."""
    from services.track_service import detect_track_type_from_title
    try:
        from mutagen.mp3 import MP3
        from mutagen.id3 import ID3
        audio = MP3(local_path, ID3=ID3)
        tags = audio.tags
        if tags and 'TIT2' in tags:
            title = str(tags['TIT2'].text[0])
            t = detect_track_type_from_title(title)
            if t:
                return t
    except Exception:
        pass
    return detect_track_type_from_title(filename) or 'Intro Clean'


def reprocess(filename, dry_run=False):
    print('=' * 60)
    print(f'  REPROCESS TRACK')
    print(f'  File: {filename}')
    print(f'  Mode: {"DRY RUN" if dry_run else "LIVE"}')
    print('=' * 60)

    # ── Step 1: Dropbox auth ──
    token = get_valid_dropbox_token()
    team_id = DROPBOX_TEAM_MEMBER_ID
    namespace_id = get_namespace(token, team_id) if team_id else ''
    print(f'Namespace: {namespace_id}')

    # ── Step 2: Find file ──
    dropbox_path = search_dropbox(filename, token, team_id, namespace_id)
    if not dropbox_path:
        print('❌ Cannot proceed — file not found in Dropbox.')
        return False

    # ── Step 3: Get temporary direct link (for S3 upload via database_service) ──
    temp_link = get_temp_link(dropbox_path, token, team_id, namespace_id)
    if not temp_link:
        print('❌ Cannot proceed — failed to get Dropbox temporary link.')
        return False

    # ── Step 4: Download locally to read ID3 tags ──
    local_path, _ = download_file_locally(dropbox_path, token, team_id, namespace_id)
    if not local_path:
        print('❌ Cannot proceed — download failed.')
        return False

    try:
        # ── Step 5: Detect track type ──
        track_type = detect_track_type(filename, local_path)
        print(f'🎵 Detected type: {track_type}')

        # Base name (strip extension, strip trailing BPM/key)
        from services.metadata_service import strip_trailing_bpm_and_key, clean_detected_type_from_title
        name_no_ext = os.path.splitext(filename)[0]
        # Remove leading "Artist - " if present
        if ' - ' in name_no_ext:
            name_no_ext = name_no_ext.split(' - ', 1)[1]
        name_no_ext = strip_trailing_bpm_and_key(name_no_ext)
        base_name = clean_detected_type_from_title(name_no_ext, track_type) or name_no_ext
        base_name = re.sub(r'[<>:"/\\|?*]', '', base_name).strip()

        variant_name = f'{base_name} - {track_type}'
        print(f'📝 Variant name: {variant_name}')

        # ── Step 6: Build edit_info with Dropbox temp link as the file URL ──
        edit_info = {
            'type': track_type,
            'format': 'MP3',
            'name': variant_name,
            'url': temp_link,   # absolute URL — database_service will download from here
        }

        # ── Step 7: Get BPM from ID3 or filename ──
        bpm = None
        try:
            from mutagen.mp3 import MP3 as MutagenMP3
            from mutagen.id3 import ID3 as MutagenID3
            audio = MutagenMP3(local_path, ID3=MutagenID3)
            tags = audio.tags
            if tags and 'TBPM' in tags:
                bpm = int(float(str(tags['TBPM'].text[0]).strip()))
        except Exception:
            pass
        if bpm is None:
            # Try trailing number in filename (e.g. "... 78.mp3")
            m = re.search(r'(\d{2,3})\s*\.mp3$', filename, re.IGNORECASE)
            if m:
                bpm = int(m.group(1))
        print(f'🥁 BPM: {bpm}')

        # ── Step 8: prepare_track_metadata (Deezer enrichment + full track_data) ──
        from services.metadata_service import prepare_track_metadata
        print('\n📡 Running prepare_track_metadata (Deezer lookup)...')
        track_data = prepare_track_metadata(edit_info, local_path, bpm, allow_no_deezer=True)

        if not track_data:
            print('❌ prepare_track_metadata returned None — aborting.')
            return False

        # Override Fichiers URL with temp link (it should already be set but ensure it)
        track_data['Fichiers'] = temp_link

        print('\n📋 Track data to be sent:')
        for k, v in sorted(track_data.items()):
            if k != 'Fichiers':
                print(f'  {k}: {v}')
        print(f'  Fichiers: {temp_link[:80]}...')

        if dry_run:
            print('\n✅ DRY RUN complete — no changes made.')
            return True

        # ── Step 9: Push to database + S3 ──
        from services.metadata_service import send_track_info_to_api
        print('\n🚀 Sending to database + S3...')
        result = send_track_info_to_api(track_data)

        if result and 'error' not in result:
            print(f'\n✅ SUCCESS: {variant_name}')
            print(f'   Track ID: {result.get("id", "N/A")}')
            print(f'   Action:   {result.get("action", "N/A")}')
        else:
            err = result.get('error') if result else 'Unknown error'
            print(f'\n❌ FAILED: {err}')
            return False

        return True

    finally:
        # Clean up temp file
        try:
            os.unlink(local_path)
            print(f'\n🗑️  Cleaned up temp file.')
        except Exception:
            pass


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Reprocess a single Dropbox track through the full pipeline.'
    )
    parser.add_argument('filename', help='Dropbox filename (e.g. "Adele - Hello (Intro Clean) 78.mp3")')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without uploading/updating DB')
    args = parser.parse_args()

    ok = reprocess(args.filename, dry_run=args.dry_run)
    sys.exit(0 if ok else 1)
