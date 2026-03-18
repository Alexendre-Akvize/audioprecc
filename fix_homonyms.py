#!/usr/bin/env python3
"""
Fix Homonyms — Find tracks with the same title but different artists,
detect wrong artist associations, and fix them.

Uses bulk SQL queries for speed (single query instead of 16K individual ones).

Usage:
    python3 -u fix_homonyms.py                   # Dry-run: report mismatches
    python3 -u fix_homonyms.py --fix             # Apply fixes (disconnect wrong artists)
    python3 -u fix_homonyms.py --scan-dropbox    # Also scan Dropbox for homonyms
"""

import os
import re
import sys
import json
import argparse
import unicodedata
from collections import defaultdict
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DROPBOX_TEAM_MEMBER_ID
from services.dropbox_service import get_valid_dropbox_token
from services.metadata_service import strip_trailing_bpm_and_key


# ─── Helpers ──────────────────────────────────────────────────────────────────

def norm(text):
    """Lowercase, strip accents, remove punctuation and hyphens — for fuzzy comparison."""
    if not text:
        return ''
    s = unicodedata.normalize('NFKD', str(text).lower())
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9\s]', '', s)
    return ' '.join(s.split())


def norm_squash(text):
    """Like norm() but also removes spaces — catches 'jay z' == 'jayz' etc."""
    return norm(text).replace(' ', '')


def primary_artist(artist_string):
    """Return the first artist name (before feat / ft / &)."""
    if not artist_string:
        return ''
    s = re.sub(r'\s+(?:feat\.?|ft\.?|featuring)\s+.*$', '', artist_string, flags=re.IGNORECASE)
    s = re.sub(r'\s+&\s+.*$', '', s)
    s = re.sub(r'\s+x\s+.*$', '', s, flags=re.IGNORECASE)
    return s.strip()


def split_artist(name):
    """Split compound artist into normalized individual name parts."""
    if not name:
        return set()
    s = re.sub(r'\s+(?:feat\.?|ft\.?|featuring)\s+', '|', name, flags=re.IGNORECASE)
    s = re.sub(r'\s+&\s+', '|', s)
    s = re.sub(r'\s+x\s+', '|', s, flags=re.IGNORECASE)
    s = re.sub(r'\s*,\s+', '|', s)
    return {norm(p.strip()) for p in s.split('|') if p.strip()}


def words_overlap(a, b):
    """Check if two normalized strings share at least one word."""
    return bool(set(a.split()) & set(b.split()))


def artist_belongs(artist_name_norm, original_artist_str):
    """Check if a connected artist plausibly belongs to this track."""
    if not artist_name_norm or not original_artist_str:
        return True  # can't tell, assume ok
    parts = split_artist(original_artist_str)
    # Exact normalized match
    if artist_name_norm in parts:
        return True
    # Squashed match (jay z == jayz, e 40 == e40, street life == streetlife)
    squashed = norm_squash(artist_name_norm)
    if any(squashed == norm_squash(p) for p in parts):
        return True
    # Word overlap
    if any(words_overlap(artist_name_norm, p) for p in parts):
        return True
    # Full original-artist squashed comparison (handles "Salt-N-Pepa" vs "saltnpepa")
    if squashed == norm_squash(original_artist_str):
        return True
    return False


# ─── Dropbox scanner (optional) ──────────────────────────────────────────────

def parse_filename(filename):
    """Extract (artist, title) from a DJ-pool filename."""
    name = os.path.splitext(filename)[0]
    if ' - ' not in name:
        return None, name
    artist_part, title_part = name.split(' - ', 1)
    title_part = strip_trailing_bpm_and_key(title_part)
    title_part = re.sub(
        r'\s*[\(\[]\s*(?:clean|dirty|inst(?:rumental)?|'
        r'quick\s*hit(?:\s+(?:clean|dirty))?|'
        r'(?:[\w\s]*\s+)?intro(?:\s+(?:clean|dirty))?(?:\s*-\s*(?:clean|dirty))?|'
        r'(?:[\w\s]*\s+)?acap(?:ella)?\s*(?:in(?:tro)?|out(?:ro)?)?'
        r'(?:\s*[&+]\s*(?:acap(?:ella)?\s*)?(?:in(?:tro)?|out(?:ro)?))?|'
        r'acap(?:ella)?\s*loop|clapapella|verse|perfect\s*version|'
        r'short|extended|radio\s*edit)\s*[\)\]]',
        '', title_part, flags=re.IGNORECASE,
    )
    edit_kws = ['remix', 'edit', 'intro', 'outro', 'transition', 'hype',
                'club', 'bootleg', 'mashup', 'blend', 'rework', 'redrum',
                'flip', 'version', 'mix', 'dub', 'vip', 'slam', 'acap']
    if ' - ' in title_part:
        parts = title_part.rsplit(' - ', 1)
        if any(kw in parts[1].lower() for kw in edit_kws):
            title_part = parts[0]
    return artist_part.strip(), re.sub(r'\s+', ' ', title_part).strip()


def scan_dropbox(folder_path=''):
    """Recursively list every MP3/WAV on Dropbox."""
    token = get_valid_dropbox_token()
    if not token:
        print('ERROR: no Dropbox token.')
        return []
    team_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    if team_id:
        headers['Dropbox-API-Select-User'] = team_id
    ns = ''
    if team_id:
        try:
            r = requests.post('https://api.dropboxapi.com/2/users/get_current_account',
                              headers={'Authorization': f'Bearer {token}',
                                       'Dropbox-API-Select-User': team_id})
            if r.status_code == 200:
                ns = r.json().get('root_info', {}).get('root_namespace_id', '')
        except Exception:
            pass
    if ns:
        headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': ns})
    if folder_path and not folder_path.startswith('/'):
        folder_path = '/' + folder_path
    if folder_path == '/':
        folder_path = ''

    files, has_more, cursor = [], True, None
    print(f'Scanning Dropbox "{folder_path or "(root)"}" recursively...')
    while has_more:
        try:
            if cursor:
                resp = requests.post('https://api.dropboxapi.com/2/files/list_folder/continue',
                                     headers=headers, json={'cursor': cursor}, timeout=60)
            else:
                resp = requests.post('https://api.dropboxapi.com/2/files/list_folder',
                                     headers=headers,
                                     json={'path': folder_path, 'recursive': True,
                                           'include_media_info': False, 'include_deleted': False,
                                           'limit': 2000}, timeout=60)
        except requests.RequestException as e:
            print(f'  Network error: {e}')
            break
        if resp.status_code != 200:
            print(f'  Dropbox API error {resp.status_code}: {resp.text[:300]}')
            break
        result = resp.json()
        for entry in result.get('entries', []):
            if entry.get('.tag') == 'file':
                n = entry.get('name', '')
                if n.lower().endswith(('.mp3', '.wav')):
                    files.append({'name': n, 'path': entry.get('path_display', ''),
                                  'folder': os.path.dirname(entry.get('path_display', ''))})
        has_more = result.get('has_more', False)
        cursor = result.get('cursor')
        if len(files) % 500 < 50:
            print(f'  ... {len(files)} audio files so far')
    print(f'Scan complete: {len(files)} audio files.\n')
    return files


# ─── Database: find & fix via bulk SQL ────────────────────────────────────────

def find_and_fix(apply_fix=False):
    """
    1. SQL: get all Artist connections for homonym tracks
    2. SQL: get all ReferenceArtist connections for homonym tracks
    3. Check each connection in Python
    4. Optionally disconnect wrong ones
    """
    try:
        from prisma import Prisma
    except ImportError:
        print('ERROR: prisma not available.')
        return []

    db = Prisma()
    db.connect()
    print('Connected to database.\n')

    # ── Step 1: Find homonym titles ──
    print('Step 1/3: Finding homonym titles (SQL)...')
    homonym_sql = '''
        SELECT LOWER(TRIM(title)) AS norm_title
        FROM "Track"
        WHERE status = 'approved'
          AND title IS NOT NULL AND TRIM(title) != ''
          AND "originalArtist" IS NOT NULL AND TRIM("originalArtist") != ''
        GROUP BY LOWER(TRIM(title))
        HAVING COUNT(DISTINCT LOWER(TRIM("originalArtist"))) >= 2
    '''
    homonym_rows = db.query_raw(homonym_sql)
    homonym_titles = {r['norm_title'] for r in homonym_rows}
    print(f'  {len(homonym_titles)} homonym titles found.\n')

    if not homonym_titles:
        db.disconnect()
        return []

    # ── Step 2: Bulk-fetch all Artist connections for homonym tracks ──
    print('Step 2/3: Loading artist connections for homonym tracks (SQL)...')

    artist_rows = db.query_raw('''
        SELECT t.id       AS track_id,
               t.title,
               t."originalArtist",
               a.id       AS artist_id,
               a.name     AS artist_name,
               'Artist'   AS rel_type
        FROM "Track" t
        JOIN "_Artist_tracks" link ON link."B" = t.id
        JOIN "Artist" a            ON a.id = link."A"
        WHERE t.status = 'approved'
          AND LOWER(TRIM(t.title)) IN (
              SELECT LOWER(TRIM(title))
              FROM "Track"
              WHERE status = 'approved'
                AND title IS NOT NULL AND TRIM(title) != ''
                AND "originalArtist" IS NOT NULL AND TRIM("originalArtist") != ''
              GROUP BY LOWER(TRIM(title))
              HAVING COUNT(DISTINCT LOWER(TRIM("originalArtist"))) >= 2
          )
    ''')
    print(f'  {len(artist_rows)} Artist connections loaded.')

    ref_artist_rows = db.query_raw('''
        SELECT t.id       AS track_id,
               t.title,
               t."originalArtist",
               ra.id      AS artist_id,
               ra.name    AS artist_name,
               'ReferenceArtist' AS rel_type
        FROM "Track" t
        JOIN "_ReferenceArtist_tracks" link ON link."B" = t.id
        JOIN "ReferenceArtist" ra           ON ra.id = link."A"
        WHERE t.status = 'approved'
          AND LOWER(TRIM(t.title)) IN (
              SELECT LOWER(TRIM(title))
              FROM "Track"
              WHERE status = 'approved'
                AND title IS NOT NULL AND TRIM(title) != ''
                AND "originalArtist" IS NOT NULL AND TRIM("originalArtist") != ''
              GROUP BY LOWER(TRIM(title))
              HAVING COUNT(DISTINCT LOWER(TRIM("originalArtist"))) >= 2
          )
    ''')
    print(f'  {len(ref_artist_rows)} ReferenceArtist connections loaded.\n')

    # ── Step 3: Check each connection ──
    print('Step 3/3: Checking for mismatches...\n')

    all_connections = artist_rows + ref_artist_rows
    mismatches = []

    for row in all_connections:
        original_artist = (row['originalArtist'] or '').strip()
        connected_name = (row['artist_name'] or '').strip()
        connected_norm = norm(connected_name)

        if not original_artist or not connected_norm:
            continue

        if artist_belongs(connected_norm, original_artist):
            continue

        mismatches.append({
            'track_id': row['track_id'],
            'title': row['title'],
            'original_artist': original_artist,
            'connected_id': row['artist_id'],
            'connected_name': connected_name,
            'rel_type': row['rel_type'],
        })

    # Group by track for readable output
    by_track = defaultdict(list)
    for mm in mismatches:
        by_track[mm['track_id']].append(mm)

    print(f'Found {len(mismatches)} mismatches across {len(by_track)} tracks.\n')

    # ── Display & optionally fix ──
    report = []
    fixed_count = 0

    for track_id, track_mismatches in sorted(by_track.items(), key=lambda x: x[1][0]['title'].lower()):
        first = track_mismatches[0]
        print(f'  "{first["title"]}" (originalArtist: {first["original_artist"]})')

        entry = {
            'track_id': track_id,
            'title': first['title'],
            'original_artist': first['original_artist'],
            'mismatches': [],
            'fixed': False,
        }

        for mm in track_mismatches:
            label = f'    WRONG {mm["rel_type"]}: "{mm["connected_name"]}"'
            entry['mismatches'].append({
                'type': mm['rel_type'],
                'id': mm['connected_id'],
                'name': mm['connected_name'],
            })

            if apply_fix:
                try:
                    if mm['rel_type'] == 'Artist':
                        db.track.update(
                            where={'id': track_id},
                            data={'Artist': {'disconnect': [{'id': mm['connected_id']}]}},
                        )
                    else:
                        db.track.update(
                            where={'id': track_id},
                            data={'ReferenceArtist': {'disconnect': [{'id': mm['connected_id']}]}},
                        )
                    print(f'{label}  --> DISCONNECTED')
                    entry['fixed'] = True
                    fixed_count += 1
                except Exception as err:
                    print(f'{label}  --> FAILED: {err}')
            else:
                print(label)

        report.append(entry)

    db.disconnect()

    print(f'\n{"=" * 60}')
    print(f'  Total mismatches: {len(mismatches)}')
    print(f'  Tracks affected: {len(by_track)}')
    if apply_fix:
        print(f'  Fixed: {fixed_count}')
    print(f'{"=" * 60}\n')

    return report


# ─── Database: find ALL wrong ReferenceArtist connections ─────────────────────

def find_and_fix_all_ref_artists(apply_fix=False):
    """
    Find ALL tracks where a connected ReferenceArtist doesn't match the
    track's originalArtist. This catches:
      - Remixes/edits wrongly linked to the sampled artist (e.g. Adele remixes)
      - Homonym mismatches (same title, different artist)
      - Any other wrong ReferenceArtist connection

    Much broader than homonym-only detection.
    """
    try:
        from prisma import Prisma
    except ImportError:
        print('ERROR: prisma not available.')
        return []

    db = Prisma()
    db.connect()
    print('Connected to database.\n')

    REMIX_KEYWORDS_RE = re.compile(
        r'\b(remix|edit|mashup|bootleg|flip|rework|redrum|blend|remake)\b',
        re.IGNORECASE,
    )

    # Bulk-fetch ALL ReferenceArtist connections with track metadata
    print('Loading all ReferenceArtist connections (SQL)...')
    rows = db.query_raw('''
        SELECT t.id          AS track_id,
               t.title,
               t."originalArtist",
               t."ISRC",
               t."trackFile_filename" AS filename,
               ra.id         AS ra_id,
               ra.name       AS ra_name
        FROM "Track" t
        JOIN "_ReferenceArtist_tracks" link ON link."B" = t.id
        JOIN "ReferenceArtist" ra           ON ra.id = link."A"
        WHERE t.status = 'approved'
    ''')
    print(f'  {len(rows)} ReferenceArtist connections loaded.\n')

    print('Loading all Artist connections (SQL)...')
    artist_rows = db.query_raw('''
        SELECT t.id          AS track_id,
               t.title,
               t."originalArtist",
               t."trackFile_filename" AS filename,
               a.id          AS a_id,
               a.name        AS a_name
        FROM "Track" t
        JOIN "_Artist_tracks" link ON link."B" = t.id
        JOIN "Artist" a            ON a.id = link."A"
        WHERE t.status = 'approved'
    ''')
    print(f'  {len(artist_rows)} Artist connections loaded.\n')

    print('Checking for mismatches...\n')

    mismatches = []
    empty_artist_skipped = 0

    for row in rows:
        original_artist = (row['originalArtist'] or '').strip()
        ra_name = (row['ra_name'] or '').strip()
        ra_norm = norm(ra_name)

        if not ra_norm:
            continue

        effective_artist = original_artist

        if not effective_artist:
            filename = (row.get('filename') or '').strip()
            if filename:
                file_artist, _ = parse_filename(filename)
                if file_artist:
                    effective_artist = file_artist

        if not effective_artist:
            title = (row.get('title') or '').strip()
            if REMIX_KEYWORDS_RE.search(title):
                mismatches.append({
                    'track_id': row['track_id'],
                    'title': row['title'],
                    'original_artist': '(empty — remix/edit)',
                    'connected_id': row['ra_id'],
                    'connected_name': ra_name,
                    'rel_type': 'ReferenceArtist',
                })
            else:
                empty_artist_skipped += 1
            continue

        if artist_belongs(ra_norm, effective_artist):
            continue

        mismatches.append({
            'track_id': row['track_id'],
            'title': row['title'],
            'original_artist': effective_artist,
            'connected_id': row['ra_id'],
            'connected_name': ra_name,
            'rel_type': 'ReferenceArtist',
        })

    for row in artist_rows:
        original_artist = (row['originalArtist'] or '').strip()
        a_name = (row['a_name'] or '').strip()
        a_norm = norm(a_name)

        if not a_norm:
            continue

        effective_artist = original_artist

        if not effective_artist:
            filename = (row.get('filename') or '').strip()
            if filename:
                file_artist, _ = parse_filename(filename)
                if file_artist:
                    effective_artist = file_artist

        if not effective_artist:
            empty_artist_skipped += 1
            continue

        if artist_belongs(a_norm, effective_artist):
            continue

        mismatches.append({
            'track_id': row['track_id'],
            'title': row['title'],
            'original_artist': effective_artist,
            'connected_id': row['a_id'],
            'connected_name': a_name,
            'rel_type': 'Artist',
        })

    by_track = defaultdict(list)
    for mm in mismatches:
        by_track[mm['track_id']].append(mm)

    print(f'Found {len(mismatches)} mismatches across {len(by_track)} tracks.')
    if empty_artist_skipped:
        print(f'  ({empty_artist_skipped} connections skipped — empty originalArtist, no filename, non-remix title)')
    print()

    report = []
    fixed_count = 0

    for track_id, track_mms in sorted(by_track.items(), key=lambda x: x[1][0]['title'].lower()):
        first = track_mms[0]
        print(f'  "{first["title"]}" (originalArtist: {first["original_artist"]})')

        entry = {
            'track_id': track_id,
            'title': first['title'],
            'original_artist': first['original_artist'],
            'mismatches': [],
            'fixed': False,
        }

        for mm in track_mms:
            label = f'    WRONG {mm["rel_type"]}: "{mm["connected_name"]}"'
            entry['mismatches'].append({
                'type': mm['rel_type'],
                'id': mm['connected_id'],
                'name': mm['connected_name'],
            })

            if apply_fix:
                try:
                    if mm['rel_type'] == 'Artist':
                        db.track.update(
                            where={'id': track_id},
                            data={'Artist': {'disconnect': [{'id': mm['connected_id']}]}},
                        )
                    else:
                        db.track.update(
                            where={'id': track_id},
                            data={'ReferenceArtist': {'disconnect': [{'id': mm['connected_id']}]}},
                        )
                    print(f'{label}  --> DISCONNECTED')
                    entry['fixed'] = True
                    fixed_count += 1
                except Exception as err:
                    print(f'{label}  --> FAILED: {err}')
            else:
                print(label)

        report.append(entry)

    db.disconnect()

    print(f'\n{"=" * 60}')
    print(f'  Total mismatches: {len(mismatches)}')
    print(f'  Tracks affected: {len(by_track)}')
    if apply_fix:
        print(f'  Fixed: {fixed_count}')
    print(f'{"=" * 60}\n')

    return report


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Detect and fix homonym track misplacements.')
    parser.add_argument('--fix', action='store_true',
                        help='Apply fixes (disconnect wrong artist associations)')
    parser.add_argument('--mode', choices=['homonyms', 'all'], default='all',
                        help='"homonyms" = only titles shared by 2+ artists; '
                             '"all" = check every track connection (default)')
    parser.add_argument('--scan-dropbox', action='store_true',
                        help='Also scan Dropbox for homonym files')
    parser.add_argument('--folder', default='',
                        help='Dropbox folder to scan (default: entire Dropbox)')
    args = parser.parse_args()

    print('=' * 60)
    print('  HOMONYM DETECTOR & FIXER')
    print(f'  Mode: {"FIX" if args.fix else "DRY RUN (--fix to apply)"}')
    print(f'  Scope: {args.mode.upper()}')
    print(f'  Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60 + '\n')

    # Optional Dropbox scan
    if args.scan_dropbox:
        files = scan_dropbox(args.folder)
        if files:
            title_groups = defaultdict(list)
            for f in files:
                a, t = parse_filename(f['name'])
                if a and t:
                    n = norm(t)
                    if n:
                        title_groups[n].append({'artist': a, 'title': t,
                                                'file': f['name'], 'path': f['path']})
            homonyms = {k: v for k, v in title_groups.items()
                        if len({norm(primary_artist(e['artist'])) for e in v}) >= 2}
            print(f'Dropbox homonyms: {len(homonyms)} titles\n')
            for i, (_, entries) in enumerate(sorted(homonyms.items()), 1):
                artists = sorted({primary_artist(e['artist']) for e in entries})
                print(f'  {i}. "{entries[0]["title"]}" — artists: {", ".join(artists)}')
            print()

    # Main DB check
    if args.mode == 'all':
        report = find_and_fix_all_ref_artists(apply_fix=args.fix)
    else:
        report = find_and_fix(apply_fix=args.fix)

    if not report:
        print('No mismatches found. Everything looks correct!')
    elif not args.fix:
        print('Run with --fix to disconnect wrong artist associations.')

    print('Done.')


if __name__ == '__main__':
    main()
