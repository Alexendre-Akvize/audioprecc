#!/usr/bin/env python3
"""
fix_s3_homonyms.py — Migrate all homonym tracks from old flat S3 paths to
ISRC-prefixed paths so files are never overwritten by tracks with the same title.

PROBLEM:
  tracks/mp3/Hello - Intro.mp3      ← shared by all "Hello" tracks → last upload wins
FIXED:
  tracks/mp3/GBBKS1500214/Hello - Intro.mp3   ← unique per ISRC → no collision

SCOPE:
  - Only tracks in homonym groups (same title, different originalArtist)
  - Only file fields that still use the old flat format (no "/" in the filename)
  - Both MP3 and WAV variants for every file type

Usage:
    python3 -u fix_s3_homonyms.py --dry-run            # Report only
    python3 -u fix_s3_homonyms.py --fix                # Apply all fixes
    python3 -u fix_s3_homonyms.py --fix --artist Adele # Limit to one artist
    python3 -u fix_s3_homonyms.py --fix --isrc GBBKS1500214
"""

import os, sys, re, json, time, argparse, tempfile, traceback
from collections import defaultdict
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from config import DROPBOX_TEAM_MEMBER_ID
from services.dropbox_service import get_valid_dropbox_token


# ─── Field definitions ────────────────────────────────────────────────────────
# Each tuple: (mp3_field, wav_field, variant_label)
# wav_field may be None if no WAV counterpart exists for that variant.
FIELD_PAIRS = [
    ('trackFile_filename',            'trackWav_filename',               'Main'),
    ('intro_filename',                'introWav_filename',               'Intro'),
    ('short_filename',                'shortWav_filename',               'Short'),
    ('shortMain_filename',            'shortMainWav_filename',           'Short Main'),
    ('extendedTrackMp3_filename',     'extendedTrackWave_filename',      'Extended'),
    ('extendedTrackMp3Clean_filename','extendedTrackWaveClean_filename', 'Extended Clean'),
    ('extendedTrackMp3Dirty_filename','extendedTrackWaveDirty_filename', 'Extended Dirty'),
    ('acapella_filename',             'acapellaWav_filename',            'Acapella'),
    ('instru_filename',               'instruWav_filename',              'Instrumental'),
    ('acapIn_filename',               'acapInWav_filename',              'Acap In'),
    ('acapOut_filename',              'acapOutWav_filename',             'Acap Out'),
    ('acapInAcapOutMain_filename',    'acapInAcapOutMainWav_filename',   'Acap In Acap Out'),
    ('clapInMain_filename',           'clapInMainWav_filename',          'Clap In Main'),
    ('clapInShortAcapOut_filename',   'clapInShortAcapOutWav_filename',  'Clap In Short Acap Out'),
    ('slamDirtyMain_filename',        'slamDirtyMainWav_filename',       'Slam Dirty Main'),
    ('slamIntroShortAcapOut_filename','slamIntroShortAcapOutWav_filename','Slam Intro Short Acap Out'),
    ('shortAcapIn_filename',          'shortAcapInWav_filename',         'Short Acap In'),
    ('shortAcapOut_filename',         'shortAcapOutWav_filename',        'Short Acap Out'),
    ('shortClapIn_filename',          'shortClapInWav_filename',         'Short Clap In'),
    ('superShort_filename',           'superShortWav_filename',          'Super Short'),
    ('originalTrackMp3_filename',     'originalTrackWave_filename',      'Original'),
    ('originalTrackMp3Clean_filename','originalTrackWaveClean_filename', 'Original Clean'),
    ('originalTrackMp3Dirty_filename','originalTrackWaveDirty_filename', 'Original Dirty'),
    ('originalTrackMp3Main_filename', None,                              'Original Main'),
    ('mixedTrackMp3_filename',        'mixedTrackWave_filename',         'Mixed'),
    ('trackPreview_filename',         None,                              'Preview'),
]

MP3_FIELDS  = {p[0] for p in FIELD_PAIRS if p[0]}
WAV_FIELDS  = {p[1] for p in FIELD_PAIRS if p[1]}
MP3_TO_WAV  = {p[0]: p[1]  for p in FIELD_PAIRS if p[0] and p[1]}
WAV_TO_MP3  = {p[1]: p[0]  for p in FIELD_PAIRS if p[0] and p[1]}
FIELD_LABEL = {p[0]: p[2]  for p in FIELD_PAIRS if p[0]}
FIELD_LABEL.update({p[1]: p[2] for p in FIELD_PAIRS if p[1]})

AUDIO_MP3_PATH = 'tracks/mp3'
AUDIO_WAV_PATH = 'tracks/wav'


# ─── Helpers ─────────────────────────────────────────────────────────────────

def is_old_format(filename):
    """True if the filename still uses the old flat format (no ISRC sub-folder)."""
    return filename and '/' not in filename


def safe_s3_folder(isrc, base_track_id=''):
    """Build a safe S3 sub-folder name from ISRC or trackId."""
    raw = isrc if isrc else base_track_id
    if not raw:
        return ''
    return re.sub(r'[^\w.\-]', '_', raw).strip('_')


# ─── Dropbox helpers ──────────────────────────────────────────────────────────

_dbx_token = None
_dbx_namespace = None

def get_dbx(team_id):
    global _dbx_token, _dbx_namespace
    if not _dbx_token:
        _dbx_token = get_valid_dropbox_token()
        if team_id:
            try:
                r = requests.post(
                    'https://api.dropboxapi.com/2/users/get_current_account',
                    headers={'Authorization': f'Bearer {_dbx_token}',
                             'Dropbox-API-Select-User': team_id},
                    timeout=15,
                )
                if r.status_code == 200:
                    _dbx_namespace = r.json().get('root_info', {}).get('root_namespace_id', '')
            except Exception:
                pass
    return _dbx_token, _dbx_namespace


def dbx_headers(token, team_id, namespace):
    h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    if team_id:
        h['Dropbox-API-Select-User'] = team_id
    if namespace:
        h['Dropbox-API-Path-Root'] = json.dumps(
            {'.tag': 'namespace_id', 'namespace_id': namespace}
        )
    return h


def search_dropbox_for_track(artist, title, token, team_id, namespace):
    """
    Search Dropbox /track done/ for files matching artist + title.
    Returns list of {path, name} dicts.
    """
    query = f'{artist} {title}'
    headers = dbx_headers(token, team_id, namespace)
    results = []
    try:
        r = requests.post(
            'https://api.dropboxapi.com/2/files/search_v2',
            headers=headers,
            json={
                'query': query,
                'options': {
                    'path': '/track done',
                    'max_results': 50,
                    'file_status': 'active',
                    'filename_only': False,
                },
            },
            timeout=30,
        )
        if r.status_code == 200:
            for m in r.json().get('matches', []):
                meta = m.get('metadata', {}).get('metadata', {})
                if meta.get('.tag') == 'file':
                    name = meta.get('name', '')
                    if name.lower().endswith(('.mp3', '.wav')):
                        results.append({
                            'name': name,
                            'path': meta.get('path_display', ''),
                            'size': meta.get('size', 0),
                        })
    except Exception as e:
        print(f'    ⚠️  Dropbox search error: {e}')
    return results


def get_temp_link(path, token, team_id, namespace):
    headers = dbx_headers(token, team_id, namespace)
    r = requests.post(
        'https://api.dropboxapi.com/2/files/get_temporary_link',
        headers=headers,
        json={'path': path},
        timeout=15,
    )
    if r.status_code == 200:
        return r.json().get('link', '')
    return None


def download_from_dropbox(path, token, team_id, namespace):
    """Download a Dropbox file to a temp local path. Returns local_path."""
    h = {
        'Authorization': f'Bearer {token}',
        'Dropbox-API-Arg': json.dumps({'path': path}),
    }
    if team_id:
        h['Dropbox-API-Select-User'] = team_id
    if namespace:
        h['Dropbox-API-Path-Root'] = json.dumps(
            {'.tag': 'namespace_id', 'namespace_id': namespace}
        )
    r = requests.post(
        'https://content.dropboxapi.com/2/files/download',
        headers=h, stream=True, timeout=300,
    )
    if r.status_code != 200:
        return None
    ext = '.wav' if path.lower().endswith('.wav') else '.mp3'
    tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
    for chunk in r.iter_content(65536):
        if chunk:
            tmp.write(chunk)
    tmp.flush(); tmp.close()
    return tmp.name


# ─── S3 helpers ───────────────────────────────────────────────────────────────

def get_s3():
    from s3_service import get_s3_service, S3_BUCKET
    return get_s3_service()._client, S3_BUCKET


def s3_upload(client, bucket, s3_key, data: bytes, is_wav: bool):
    content_type = 'audio/wav' if is_wav else 'audio/mpeg'
    client.put_object(
        Bucket=bucket, Key=s3_key,
        Body=data, ContentType=content_type, ACL='public-read',
    )


def s3_download(client, bucket, s3_key):
    try:
        resp = client.get_object(Bucket=bucket, Key=s3_key)
        return resp['Body'].read()
    except Exception:
        return None


# ─── Audio + tag helpers ──────────────────────────────────────────────────────

def embed_tags_mp3(local_mp3, isrc, title, artist, bpm, track_id):
    """
    Clean all DJ-pool branding tags and embed the ID By Rivoli cover + proper metadata.
    Re-uses update_metadata() from the pipeline.
    """
    from services.metadata_service import update_metadata
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TSRC, TXXX

    # update_metadata reads a source file for tags and writes to output file.
    # Pass the same file for both (it reads first, then rewrites).
    src_tmp = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
    with open(local_mp3, 'rb') as f:
        src_tmp.write(f.read())
    src_tmp.close()

    try:
        update_metadata(local_mp3, artist, title, src_tmp.name, bpm)

        # Fix ISRC and TRACK_ID (update_metadata may miss them if original had none)
        tags = ID3(local_mp3)
        tags.delall('TSRC')
        tags.add(TSRC(encoding=3, text=isrc))
        tags.delall('TXXX:TRACK_ID')
        tags.add(TXXX(encoding=3, desc='TRACK_ID', text=track_id))
        tags.save(local_mp3, v1=2, v2_version=3)
    finally:
        os.unlink(src_tmp.name)


def embed_tags_wav(local_wav, isrc, title, artist, bpm, track_id, source_mp3=None):
    """
    Embed ID3 tags + ID By Rivoli cover into a WAV file.
    source_mp3 is an optional local MP3 path used to read original tags.
    """
    from services.metadata_service import update_metadata_wav
    from mutagen.wave import WAVE
    from mutagen.id3 import TSRC, TXXX

    ref_path = source_mp3 if source_mp3 and os.path.exists(source_mp3) else local_wav
    update_metadata_wav(local_wav, artist, title, ref_path, bpm)

    # Fix ISRC + TRACK_ID
    try:
        wav = WAVE(local_wav)
        if wav.tags is None:
            wav.add_tags()
        wav.tags.delall('TSRC')
        wav.tags.add(TSRC(encoding=3, text=isrc))
        wav.tags.delall('TXXX:TRACK_ID')
        wav.tags.add(TXXX(encoding=3, desc='TRACK_ID', text=track_id))
        wav.save()
    except Exception as e:
        print(f'    ⚠️  WAV TSRC/TRACK_ID patch failed: {e}')


def mp3_to_wav(mp3_path):
    """Convert an MP3 to WAV using pydub. Returns local WAV path."""
    from pydub import AudioSegment
    audio = AudioSegment.from_mp3(mp3_path)
    tmp = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
    tmp.close()
    audio.export(tmp.name, format='wav')
    return tmp.name


# ─── Variant matching ─────────────────────────────────────────────────────────

def match_dropbox_file_to_field(dropbox_name, track):
    """
    Given a Dropbox filename (e.g. 'Adele - Hello (Intro Clean) 78.mp3'),
    return the (mp3_field, wav_field) tuple that this file corresponds to,
    or (None, None) if it can't be determined.
    """
    from services.metadata_service import detect_track_type_from_title
    name_no_ext = os.path.splitext(dropbox_name)[0]
    variant_type = detect_track_type_from_title(name_no_ext)

    # Map detected variant type → DB field pair
    TYPE_TO_FIELDS = {
        'Main':                     ('trackFile_filename',            'trackWav_filename'),
        'Intro':                    ('intro_filename',                'introWav_filename'),
        'Short':                    ('short_filename',                'shortWav_filename'),
        'Short Main':               ('shortMain_filename',            'shortMainWav_filename'),
        'Extended':                 ('extendedTrackMp3_filename',     'extendedTrackWave_filename'),
        'Extended Clean':           ('extendedTrackMp3Clean_filename','extendedTrackWaveClean_filename'),
        'Extended Dirty':           ('extendedTrackMp3Dirty_filename','extendedTrackWaveDirty_filename'),
        'Acapella':                 ('acapella_filename',             'acapellaWav_filename'),
        'Instrumental':             ('instru_filename',               'instruWav_filename'),
        'Acap In':                  ('acapIn_filename',               'acapInWav_filename'),
        'Acap Out':                 ('acapOut_filename',              'acapOutWav_filename'),
        'Acap In Acap Out':         ('acapInAcapOutMain_filename',    'acapInAcapOutMainWav_filename'),
        'Clap In Main':             ('clapInMain_filename',           'clapInMainWav_filename'),
        'Clap In Short Acap Out':   ('clapInShortAcapOut_filename',   'clapInShortAcapOutWav_filename'),
        'Slam Dirty Main':          ('slamDirtyMain_filename',        'slamDirtyMainWav_filename'),
        'Slam Intro Short Acap Out':('slamIntroShortAcapOut_filename','slamIntroShortAcapOutWav_filename'),
        'Short Acap In':            ('shortAcapIn_filename',          'shortAcapInWav_filename'),
        'Short Acap Out':           ('shortAcapOut_filename',         'shortAcapOutWav_filename'),
        'Short Clap In':            ('shortClapIn_filename',          'shortClapInWav_filename'),
        'Super Short':              ('superShort_filename',           'superShortWav_filename'),
        'Original':                 ('originalTrackMp3_filename',     'originalTrackWave_filename'),
        'Original Clean':           ('originalTrackMp3Clean_filename','originalTrackWaveClean_filename'),
        'Original Dirty':           ('originalTrackMp3Dirty_filename','originalTrackWaveDirty_filename'),
        'Mixed':                    ('mixedTrackMp3_filename',        'mixedTrackWave_filename'),
    }

    if variant_type and variant_type in TYPE_TO_FIELDS:
        mp3_f, wav_f = TYPE_TO_FIELDS[variant_type]
        # Only match if the track actually has that field set in old format
        mp3_val = track.get(mp3_f, '') or ''
        if is_old_format(mp3_val):
            return mp3_f, wav_f
        # If the mp3 field is already migrated but wav isn't, still handle wav
        wav_val = track.get(wav_f, '') or '' if wav_f else ''
        if wav_f and is_old_format(wav_val):
            return mp3_f, wav_f

    return None, None


# ─── Database helpers ─────────────────────────────────────────────────────────

def get_db():
    from prisma import Prisma
    db = Prisma()
    db.connect()
    return db


def find_homonym_tracks(db, artist_filter=None, isrc_filter=None):
    """
    Return all tracks that:
    1. Are in a homonym group (same title, 2+ distinct originalArtist)
    2. Have at least one file field in old flat format (no "/" in filename)
    """
    all_file_fields = [f[0] for f in FIELD_PAIRS if f[0]] + [f[1] for f in FIELD_PAIRS if f[1]]
    file_cols = ', '.join(f't."{f}"' for f in all_file_fields)

    filter_clauses = []
    if artist_filter:
        safe = artist_filter.replace("'", "''")
        filter_clauses.append(f"LOWER(t.\"originalArtist\") = LOWER('{safe}')")
    if isrc_filter:
        safe = isrc_filter.replace("'", "''")
        filter_clauses.append(f"t.\"ISRC\" = '{safe}'")

    extra = ''
    if filter_clauses:
        extra = ' AND ' + ' AND '.join(filter_clauses)

    # Use an inline subquery so we never embed thousands of title strings in Python
    print('Fetching homonym tracks with file fields from DB...')
    rows = db.query_raw(f'''
        SELECT t.id, t.title, t."originalArtist", t."ISRC", t."trackId", t."bpm",
               {file_cols}
        FROM "Track" t
        WHERE t.status = 'approved'
          AND LOWER(TRIM(t.title)) IN (
              SELECT LOWER(TRIM(title))
              FROM "Track"
              WHERE status = 'approved'
                AND title IS NOT NULL AND TRIM(title) != \'\'
                AND "originalArtist" IS NOT NULL AND TRIM("originalArtist") != \'\'
              GROUP BY LOWER(TRIM(title))
              HAVING COUNT(DISTINCT LOWER(TRIM("originalArtist"))) >= 2
          )
          {extra}
    ''')
    print(f'  {len(rows):,} homonym tracks fetched.')

    # Keep only tracks that have at least one old-format file field
    affected = []
    for row in rows:
        has_old = any(
            is_old_format(row.get(f, '') or '')
            for f in all_file_fields
        )
        if has_old:
            affected.append(row)

    print(f'  {len(affected):,} tracks have at least one old-format file field.')
    return affected


def update_db_field(db, track_id, field, new_value):
    if new_value == '' or new_value is None:
        db.query_raw(f'UPDATE "Track" SET "{field}" = NULL WHERE id = \'{track_id}\'')
    else:
        db.query_raw(f'''
            UPDATE "Track" SET "{field}" = '{new_value.replace("'", "''")}' WHERE id = '{track_id}'
        ''')


# ─── Core migration ───────────────────────────────────────────────────────────

def migrate_track(track, dry_run, s3_client, s3_bucket, token, team_id, namespace, stats):
    """
    Migrate all old-format file fields for a single track.
    Downloads from Dropbox, re-encodes, re-uploads, updates DB.
    """
    from prisma import Prisma

    track_id = track['id']
    title    = (track.get('title') or '').strip()
    artist   = (track.get('originalArtist') or '').strip()
    isrc     = (track.get('ISRC') or '').strip()
    bpm_raw  = track.get('bpm')
    bpm      = int(bpm_raw) if bpm_raw else None
    base_id  = (track.get('trackId') or '').strip()

    s3_folder = safe_s3_folder(isrc, base_id)
    if not s3_folder:
        print(f'  ⚠️  Skipping {track_id}: no ISRC or trackId')
        stats['skipped'] += 1
        return

    # Which fields need migration?
    fields_to_fix_mp3 = [
        f for f in MP3_FIELDS
        if is_old_format(track.get(f, '') or '')
    ]

    if not fields_to_fix_mp3:
        return

    variant_label = ', '.join(FIELD_LABEL.get(f, f) for f in fields_to_fix_mp3)

    if dry_run:
        # Fast dry-run: only check S3 flat-path ISRC (read ID3 header only, ~10 KB).
        # Dropbox is NOT searched — we only report tracks that would be nulled or not found.
        problems = []
        for mp3_field in fields_to_fix_mp3:
            label  = FIELD_LABEL.get(mp3_field, mp3_field)
            old_fn = track.get(mp3_field, '') or ''
            if not old_fn or not isrc:
                stats['would_fix'] += 1
                continue
            # Fetch only the first 32 KB (enough for any ID3 v2 header)
            s3_key = f'{AUDIO_MP3_PATH}/{old_fn}'
            try:
                resp = s3_client.get_object(Bucket=s3_bucket, Key=s3_key,
                                            Range='bytes=0-32767')
                header_bytes = resp['Body'].read()
            except Exception:
                problems.append(f'     ❓  [{label}] NOT IN S3: {old_fn}')
                stats['not_found'] += 1
                continue
            try:
                from mutagen.id3 import ID3 as _ID3
                tmp = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
                tmp.write(header_bytes); tmp.flush(); tmp.close()
                _tags = _ID3(tmp.name)
                os.unlink(tmp.name)
                file_isrc   = str(_tags.get('TSRC', '')).strip()
                file_artist = str(_tags.get('TPE1', '?')).strip()
                if file_isrc and file_isrc != isrc:
                    problems.append(
                        f'     ⛔  [{label}] WRONG — flat S3 has {file_artist} '
                        f'({file_isrc}), expected {isrc} → will be NULLED'
                    )
                    stats['nulled'] = stats.get('nulled', 0) + 1
                else:
                    stats['would_fix'] += 1
            except Exception:
                stats['would_fix'] += 1  # Can't read header — assume OK

        if problems:
            print(f'\n  ⚠️  "{title}" — {artist} (ISRC: {isrc})')
            print(f'     Fields to migrate: {variant_label}')
            for p in problems:
                print(p)
        return

    print(f'\n  🎵 "{title}" — {artist} (ISRC: {isrc})')
    print(f'     Fields: {variant_label}')

    # ── Search Dropbox ──
    dbx_files = search_dropbox_for_track(artist, title, token, team_id, namespace)
    if dbx_files:
        print(f'     📂 Found {len(dbx_files)} Dropbox file(s)')
    else:
        print(f'     ℹ️  No Dropbox files — will use S3 flat-path fallback for all fields')

    db = get_db()
    processed_variants = set()

    # ── Phase 1: match Dropbox files to DB fields ──
    for dbx_file in dbx_files:
        dbx_name = dbx_file['name']
        dbx_path = dbx_file['path']

        mp3_field, wav_field = match_dropbox_file_to_field(dbx_name, track)
        if not mp3_field:
            print(f'     ⏭️  Cannot match "{dbx_name}" to a DB field — skipping')
            continue

        label = FIELD_LABEL.get(mp3_field, mp3_field)
        if label in processed_variants:
            continue

        old_mp3_fn = track.get(mp3_field, '') or ''
        old_wav_fn = track.get(wav_field, '') or '' if wav_field else ''
        new_mp3_fn = f'{s3_folder}/{os.path.basename(old_mp3_fn)}' if old_mp3_fn else f'{s3_folder}/{os.path.splitext(dbx_name)[0]} - {label}.mp3'
        new_wav_fn = f'{s3_folder}/{os.path.basename(old_wav_fn)}' if old_wav_fn else (f'{s3_folder}/{os.path.splitext(dbx_name)[0]} - {label}.wav' if wav_field else '')

        print(f'     ▶ [{label}] Processing from Dropbox: "{dbx_name}"')
        ok = _process_mp3_wav(
            source='dropbox', source_ref=dbx_path,
            mp3_field=mp3_field, wav_field=wav_field,
            old_mp3_fn=old_mp3_fn, old_wav_fn=old_wav_fn,
            new_mp3_fn=new_mp3_fn, new_wav_fn=new_wav_fn,
            title=title, label=label, artist=artist,
            isrc=isrc, bpm=bpm, track_id=track_id,
            s3_client=s3_client, s3_bucket=s3_bucket,
            token=token, team_id=team_id, namespace=namespace,
            db=db, stats=stats,
        )
        if ok:
            processed_variants.add(label)

    # ── Phase 2: S3 flat-path fallback for fields not covered by Dropbox ──
    # Demucs-generated files (Main, Acapella, Instrumental, Short…) live only in S3.
    # We copy them from the old flat path to the ISRC-prefixed path.
    remaining = [
        f for f in fields_to_fix_mp3
        if FIELD_LABEL.get(f, f) not in processed_variants
    ]
    if remaining:
        print(f'     🔄 S3 flat-path fallback for: {", ".join(FIELD_LABEL.get(f,f) for f in remaining)}')

    for mp3_field in remaining:
        label     = FIELD_LABEL.get(mp3_field, mp3_field)
        wav_field = MP3_TO_WAV.get(mp3_field)

        old_mp3_fn = track.get(mp3_field, '') or ''
        old_wav_fn = track.get(wav_field, '') or '' if wav_field else ''
        if not old_mp3_fn:
            continue

        new_mp3_fn = f'{s3_folder}/{old_mp3_fn}'
        new_wav_fn = f'{s3_folder}/{old_wav_fn}' if old_wav_fn else ''

        print(f'     ▶ [{label}] S3 copy: {old_mp3_fn} → {new_mp3_fn}')
        ok = _process_mp3_wav(
            source='s3', source_ref=old_mp3_fn,
            mp3_field=mp3_field, wav_field=wav_field,
            old_mp3_fn=old_mp3_fn, old_wav_fn=old_wav_fn,
            new_mp3_fn=new_mp3_fn, new_wav_fn=new_wav_fn,
            title=title, label=label, artist=artist,
            isrc=isrc, bpm=bpm, track_id=track_id,
            s3_client=s3_client, s3_bucket=s3_bucket,
            token=token, team_id=team_id, namespace=namespace,
            db=db, stats=stats,
        )
        if ok:
            processed_variants.add(label)

    db.disconnect()


def _process_mp3_wav(
    source, source_ref,
    mp3_field, wav_field,
    old_mp3_fn, old_wav_fn,
    new_mp3_fn, new_wav_fn,
    title, label, artist, isrc, bpm, track_id,
    s3_client, s3_bucket,
    token, team_id, namespace,
    db, stats,
):
    """
    Download/fetch an MP3 (from Dropbox or S3), embed tags, generate WAV,
    upload both to S3 at ISRC-prefixed paths, and update the DB.
    Returns True on success.
    """
    local_mp3 = local_wav = None
    try:
        # ── Fetch the source MP3 ──
        if source == 'dropbox':
            local_mp3 = download_from_dropbox(source_ref, token, team_id, namespace)
            if not local_mp3:
                print(f'       ❌ Dropbox download failed')
                stats['errors'] += 1
                return False
            print(f'       📥 Downloaded MP3: {os.path.getsize(local_mp3)/1024/1024:.1f} MB')
        else:
            # S3 flat-path fallback
            old_s3_key = f'{AUDIO_MP3_PATH}/{source_ref}'
            print(f'       📥 Fetching from S3: {old_s3_key}')
            mp3_bytes = s3_download(s3_client, s3_bucket, old_s3_key)
            if not mp3_bytes:
                print(f'       ❌ S3 key not found: {old_s3_key}')
                stats['not_found'] += 1
                return False
            local_mp3 = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
            local_mp3.write(mp3_bytes)
            local_mp3.flush(); local_mp3.close()
            local_mp3 = local_mp3.name
            print(f'       📥 Fetched from S3: {len(mp3_bytes)/1024/1024:.1f} MB')

            # ── ISRC safety check: reject if file belongs to a different artist ──
            if isrc:
                try:
                    from mutagen.id3 import ID3 as _ID3
                    _tags = _ID3(local_mp3)
                    file_isrc = str(_tags.get('TSRC', '')).strip()
                    if file_isrc and file_isrc != isrc:
                        print(f'       ⛔  ISRC MISMATCH — flat S3 has {file_isrc} ({str(_tags.get("TPE1","?"))}), expected {isrc}')
                        print(f'       🗑️  Nulling DB field + cleaning up any bad ISRC-prefixed file')
                        os.unlink(local_mp3); local_mp3 = None
                        # Delete ISRC-prefixed file if it already exists (from a previous bad run)
                        for _base, _fn in [
                            (AUDIO_MP3_PATH, new_mp3_fn),
                            (AUDIO_WAV_PATH, new_wav_fn),
                        ]:
                            if _fn:
                                try:
                                    s3_client.delete_object(Bucket=s3_bucket, Key=f'{_base}/{_fn}')
                                except Exception:
                                    pass
                        # Null out the DB fields so the track falls back to a valid variant
                        update_db_field(db, track_id, mp3_field, '')
                        if wav_field:
                            update_db_field(db, track_id, wav_field, '')
                        print(f'       ✅ DB fields nulled: {mp3_field}, {wav_field or ""}')
                        stats['nulled'] = stats.get('nulled', 0) + 1
                        return False
                except Exception:
                    pass  # Can't read tags — proceed with caution

        # ── Variant title (use existing filename stem) ──
        variant_title = os.path.splitext(os.path.basename(
            old_mp3_fn if old_mp3_fn else f'{title} - {label}.mp3'
        ))[0]
        track_id_tag = f'{isrc}_{re.sub(r"[ -]+", "_", variant_title)}' if isrc else variant_title

        # ── Embed tags into MP3 ──
        print(f'       🏷️  Embedding MP3 tags: "{variant_title}"')
        embed_tags_mp3(local_mp3, isrc, variant_title, artist, bpm, track_id_tag)

        # ── Upload MP3 ──
        new_s3_key_mp3 = f'{AUDIO_MP3_PATH}/{new_mp3_fn}'
        print(f'       📤 Uploading MP3 → {new_s3_key_mp3}')
        with open(local_mp3, 'rb') as f:
            s3_upload(s3_client, s3_bucket, new_s3_key_mp3, f.read(), is_wav=False)

        # ── Generate + upload WAV ──
        if wav_field:
            wav_title = os.path.splitext(os.path.basename(
                old_wav_fn if old_wav_fn else f'{title} - {label}.wav'
            ))[0]

            # Try to fetch existing WAV from S3 flat path first (preserves Demucs quality)
            local_wav = None
            if old_wav_fn and source == 's3':
                old_wav_key = f'{AUDIO_WAV_PATH}/{old_wav_fn}'
                wav_bytes = s3_download(s3_client, s3_bucket, old_wav_key)
                if wav_bytes:
                    local_wav = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
                    local_wav.write(wav_bytes)
                    local_wav.flush(); local_wav.close()
                    local_wav = local_wav.name
                    print(f'       📥 WAV fetched from S3: {len(wav_bytes)/1024/1024:.1f} MB')

            if not local_wav:
                print(f'       🔄 Converting MP3 → WAV...')
                local_wav = mp3_to_wav(local_mp3)

            embed_tags_wav(local_wav, isrc, wav_title, artist, bpm, track_id_tag, local_mp3)
            new_s3_key_wav = f'{AUDIO_WAV_PATH}/{new_wav_fn}'
            print(f'       📤 Uploading WAV → {new_s3_key_wav}')
            with open(local_wav, 'rb') as f:
                s3_upload(s3_client, s3_bucket, new_s3_key_wav, f.read(), is_wav=True)

        # ── Update DB ──
        update_db_field(db, track_id, mp3_field, new_mp3_fn)
        print(f'       ✅ DB: {mp3_field} = {new_mp3_fn}')
        if wav_field and new_wav_fn:
            update_db_field(db, track_id, wav_field, new_wav_fn)
            print(f'       ✅ DB: {wav_field} = {new_wav_fn}')

        stats['fixed'] += 1
        return True

    except Exception as e:
        print(f'       ❌ Error: {e}')
        traceback.print_exc()
        stats['errors'] += 1
        return False
    finally:
        if local_mp3 and os.path.exists(local_mp3):
            os.unlink(local_mp3)
        if local_wav and os.path.exists(local_wav):
            os.unlink(local_wav)


# ─── Single-track repair (for main app) ────────────────────────────────────────

def repair_track_by_id(track_id: str, dry_run: bool = False) -> dict:
    """
    Repair a single track by ID: migrate old flat S3 paths to ISRC-prefixed paths,
    or null fields when the flat file belongs to another artist (ISRC mismatch).

    Call from the main app (e.g. POST /repair/track/<id>).
    Returns dict: {ok: bool, message: str, fixed?: int, nulled?: int, errors?: int, not_found?: int}.
    """
    if not track_id or not str(track_id).strip():
        return {'ok': False, 'error': 'Missing track ID'}

    track_id = str(track_id).strip()
    db = get_db()
    try:
        all_file_fields = [f[0] for f in FIELD_PAIRS if f[0]] + [f[1] for f in FIELD_PAIRS if f[1]]
        file_cols = ', '.join(f't."{f}"' for f in all_file_fields)
        safe_id = track_id.replace("'", "''")
        rows = db.query_raw(f'''
            SELECT t.id, t.title, t."originalArtist", t."ISRC", t."trackId", t."bpm",
                   {file_cols}
            FROM "Track" t
            WHERE t.id = \'{safe_id}\' AND t.status = \'approved\'
        ''')
        if not rows:
            return {'ok': False, 'error': 'Track not found or not approved'}

        track = rows[0]
        has_old = any(is_old_format(track.get(f, '') or '') for f in all_file_fields)
        if not has_old:
            return {'ok': True, 'message': 'No old-format files to migrate', 'fixed': 0}

        from config import DROPBOX_TEAM_MEMBER_ID
        s3_client, s3_bucket = get_s3()
        token, namespace = get_dbx(DROPBOX_TEAM_MEMBER_ID)
        team_id = DROPBOX_TEAM_MEMBER_ID
        stats = {'fixed': 0, 'errors': 0, 'skipped': 0, 'not_found': 0, 'nulled': 0, 'would_fix': 0}
        migrate_track(track, dry_run, s3_client, s3_bucket, token, team_id, namespace, stats)

        if dry_run:
            msg = f"Would fix={stats.get('would_fix', 0)}, would_null={stats.get('nulled', 0)}, not_found={stats['not_found']}"
        else:
            msg = f"Fixed={stats['fixed']}, nulled={stats.get('nulled', 0)}, errors={stats['errors']}, not_found={stats['not_found']}"
        return {
            'ok': stats['errors'] == 0,
            'message': msg,
            'fixed': stats['fixed'],
            'nulled': stats.get('nulled', 0),
            'errors': stats['errors'],
            'not_found': stats['not_found'],
            'would_fix': stats.get('would_fix', 0),
        }
    except Exception as e:
        traceback.print_exc()
        return {'ok': False, 'error': str(e)}
    finally:
        db.disconnect()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Migrate homonym tracks from flat S3 paths to ISRC-prefixed paths.'
    )
    parser.add_argument('--fix', action='store_true',
                        help='Apply fixes (upload files, update DB). Default: dry-run.')
    parser.add_argument('--artist', default='',
                        help='Limit to a specific artist (case-insensitive).')
    parser.add_argument('--isrc', default='',
                        help='Limit to a specific ISRC.')
    parser.add_argument('--limit', type=int, default=0,
                        help='Process at most N tracks (0 = unlimited).')
    args = parser.parse_args()

    dry_run = not args.fix

    print('=' * 60)
    print('  S3 HOMONYM PATH MIGRATION')
    print(f'  Mode: {"DRY RUN" if dry_run else "LIVE FIX"}')
    if args.artist:
        print(f'  Artist filter: {args.artist}')
    if args.isrc:
        print(f'  ISRC filter: {args.isrc}')
    print(f'  Started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60 + '\n')

    # Connect to DB
    from prisma import Prisma
    db = Prisma()
    db.connect()

    tracks = find_homonym_tracks(db, args.artist or None, args.isrc or None)
    db.disconnect()

    if not tracks:
        print('✅ Nothing to migrate.')
        return

    if args.limit and args.limit > 0:
        tracks = tracks[:args.limit]
        print(f'  (limited to first {args.limit} tracks)')

    print(f'\nTotal tracks to process: {len(tracks)}')

    # Init S3 + Dropbox (needed for both dry-run checks and live fix)
    s3_client, s3_bucket = get_s3()
    token, namespace = get_dbx(DROPBOX_TEAM_MEMBER_ID)
    team_id = DROPBOX_TEAM_MEMBER_ID
    print(f'Dropbox namespace: {namespace}')
    print(f'S3 bucket: {s3_bucket}\n')

    stats = {'fixed': 0, 'errors': 0, 'skipped': 0, 'not_found': 0, 'would_fix': 0, 'nulled': 0}

    total = len(tracks)
    progress_every = max(1, min(500, total // 20))  # ~5% steps, capped at 500
    start_ts = time.time()

    for i, track in enumerate(tracks, 1):
        if i % progress_every == 0 or i == total:
            elapsed = time.time() - start_ts
            rate = i / elapsed if elapsed > 0 else 0
            eta_s = int((total - i) / rate) if rate > 0 else 0
            eta = f'{eta_s // 60}m {eta_s % 60}s' if eta_s >= 60 else f'{eta_s}s'
            if dry_run:
                print(
                    f'[{i}/{total}] '
                    f'wrong={stats.get("nulled", 0)}  '
                    f'not_found={stats["not_found"]}  '
                    f'ok={stats["would_fix"]}  '
                    f'elapsed={int(elapsed)}s  eta={eta}',
                    flush=True,
                )
            else:
                print(
                    f'\n[{i}/{total}] '
                    f'fixed={stats["fixed"]}  '
                    f'nulled={stats.get("nulled", 0)}  '
                    f'errors={stats["errors"]}  '
                    f'elapsed={int(elapsed)}s  eta={eta}\n',
                    flush=True,
                )
        try:
            migrate_track(track, dry_run, s3_client, s3_bucket,
                          token, team_id, namespace, stats)
        except Exception as e:
            print(f'  ❌ Fatal error for track {track.get("id")}: {e}')
            traceback.print_exc()
            stats['errors'] += 1

    print('\n' + '=' * 60)
    print('  MIGRATION COMPLETE')
    if dry_run:
        print(f'  Would migrate OK: {stats["would_fix"]} fields (Dropbox source or correct ISRC in S3)')
        print(f'  Wrong content:    {stats.get("nulled", 0)} fields (ISRC mismatch — will be nulled)')
        print(f'  Not found:        {stats["not_found"]} fields (no source anywhere)')
        print(f'  Run with --fix to apply.')
    else:
        print(f'  Fixed:     {stats["fixed"]}')
        print(f'  Nulled:    {stats["nulled"]} (ISRC mismatch — DB field cleared, needs re-upload)')
        print(f'  Not found: {stats["not_found"]} (not in Dropbox or S3)')
        print(f'  Errors:    {stats["errors"]}')
        print(f'  Skipped:   {stats["skipped"]}')
    print(f'  Finished:  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)


if __name__ == '__main__':
    main()
