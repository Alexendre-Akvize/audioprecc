#!/usr/bin/env python3
"""
Backfill waveforms and durations for all track variants.

Downloads audio files from the CDN, generates waveform peak data with pydub
(via waveform_generator.py), and writes the results back to the database.

Uses the same DATABASE_URL from .env and the same synchronous Prisma client
as the rest of the application (database_service.py).

Usage:
    python backfill_waveforms.py
    python backfill_waveforms.py --dry-run
    python backfill_waveforms.py --limit 10
    python backfill_waveforms.py --dry-run --limit 5
"""

import os
import sys
import json
import tempfile
import time
import argparse
import traceback

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from waveform_generator import generate_waveform


# ─── .env loader (same as the rest of the app — no python-dotenv needed) ─────

def load_env(env_path=None):
    if env_path is None:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and (
                (value[0] == '"' and value[-1] == '"')
                or (value[0] == "'" and value[-1] == "'")
            ):
                value = value[1:-1]
            os.environ.setdefault(key, value)


load_env()

CDN_BASE = os.environ.get('CDN_BASE', 'https://d1e8ujhph3mpvk.cloudfront.net')

# ─── Prisma imports (same as database_service.py) ────────────────────────────

try:
    from prisma import Prisma
    from prisma import Json as PrismaJson
    print("Prisma client available")
except ImportError as e:
    print(f"Prisma client not available: {e}")
    print("Run: pip install prisma && prisma db pull && prisma generate")
    sys.exit(1)


# ─── Audio-file → waveform / duration column mappings ────────────────────────
# Uses Prisma field names (jsonData for the main waveform, not the raw DB
# column "json"), matching database_service.py conventions.

FILE_MAPPINGS = [
    # Main edit — jsonData is the Prisma name for @map("json")
    {'file_field': 'trackWav_filename',     'json_field': 'jsonData',    'storage': 'wav', 'duration_field': 'duration'},
    {'file_field': 'trackFile_filename',    'json_field': 'jsonData',    'storage': 'mp3', 'duration_field': 'duration'},
    {'file_field': 'trackPreview_filename', 'json_field': 'previewJson', 'storage': 'mp3', 'duration_field': None},

    # Original
    {'file_field': 'originalTrackWave_filename',      'json_field': 'originalTrackWaveJson',      'storage': 'wav', 'duration_field': 'originalDuration'},
    {'file_field': 'originalTrackWaveClean_filename',  'json_field': 'originalTrackWaveCleanJson',  'storage': 'wav', 'duration_field': 'originalDuration'},
    {'file_field': 'originalTrackWaveDirty_filename',  'json_field': 'originalTrackWaveDirtyJson',  'storage': 'wav', 'duration_field': 'originalDuration'},
    {'file_field': 'originalTrackMp3_filename',        'json_field': 'originalTrackMp3Json',        'storage': 'mp3', 'duration_field': 'originalDuration'},
    {'file_field': 'originalTrackMp3Main_filename',    'json_field': 'originalTrackMp3MainJson',    'storage': 'mp3', 'duration_field': 'originalDuration'},
    {'file_field': 'originalTrackMp3Clean_filename',   'json_field': 'originalTrackMp3CleanJson',   'storage': 'mp3', 'duration_field': 'originalDuration'},
    {'file_field': 'originalTrackMp3Dirty_filename',   'json_field': 'originalTrackMp3DirtyJson',   'storage': 'mp3', 'duration_field': 'originalDuration'},

    # Extended
    {'file_field': 'extendedTrackWave_filename',      'json_field': 'extendedTrackWaveJson',      'storage': 'wav', 'duration_field': 'extendedDuration'},
    {'file_field': 'extendedTrackWaveClean_filename',  'json_field': 'extendedTrackWaveCleanJson',  'storage': 'wav', 'duration_field': 'extendedDuration'},
    {'file_field': 'extendedTrackWaveDirty_filename',  'json_field': 'extendedTrackWaveDirtyJson',  'storage': 'wav', 'duration_field': 'extendedDuration'},
    {'file_field': 'extendedTrackMp3_filename',        'json_field': 'extendedTrackMp3Json',        'storage': 'mp3', 'duration_field': 'extendedDuration'},
    {'file_field': 'extendedTrackMp3Clean_filename',   'json_field': 'extendedTrackMp3CleanJson',   'storage': 'mp3', 'duration_field': 'extendedDuration'},
    {'file_field': 'extendedTrackMp3Dirty_filename',   'json_field': 'extendedTrackMp3DirtyJson',   'storage': 'mp3', 'duration_field': 'extendedDuration'},

    # Mixed
    {'file_field': 'mixedTrackWave_filename', 'json_field': 'mixedTrackWaveJson', 'storage': 'wav', 'duration_field': 'mixedDuration'},
    {'file_field': 'mixedTrackMp3_filename',  'json_field': 'mixedTrackMp3Json',  'storage': 'mp3', 'duration_field': 'mixedDuration'},

    # Clap-in main
    {'file_field': 'clapInMainWav_filename', 'json_field': 'clapInMainWavJson', 'storage': 'wav', 'duration_field': 'clapInMainDuration'},
    {'file_field': 'clapInMain_filename',    'json_field': 'clapInMainJson',    'storage': 'mp3', 'duration_field': 'clapInMainDuration'},

    # Short main
    {'file_field': 'shortMainWav_filename', 'json_field': 'shortMainWavJson', 'storage': 'wav', 'duration_field': 'shortMainDuration'},
    {'file_field': 'shortMain_filename',    'json_field': 'shortMainJson',    'storage': 'mp3', 'duration_field': 'shortMainDuration'},

    # Short acap-in
    {'file_field': 'shortAcapInWav_filename', 'json_field': 'shortAcapInWavJson', 'storage': 'wav', 'duration_field': 'shortAcapInDuration'},
    {'file_field': 'shortAcapIn_filename',    'json_field': 'shortAcapInJson',    'storage': 'mp3', 'duration_field': 'shortAcapInDuration'},

    # Short clap-in
    {'file_field': 'shortClapInWav_filename', 'json_field': 'shortClapInWavJson', 'storage': 'wav', 'duration_field': 'shortClapInDuration'},
    {'file_field': 'shortClapIn_filename',    'json_field': 'shortClapInJson',    'storage': 'mp3', 'duration_field': 'shortClapInDuration'},

    # Acap-in acap-out main
    {'file_field': 'acapInAcapOutMainWav_filename', 'json_field': 'acapInAcapOutMainWavJson', 'storage': 'wav', 'duration_field': 'acapInAcapOutMainDuration'},
    {'file_field': 'acapInAcapOutMain_filename',    'json_field': 'acapInAcapOutMainJson',    'storage': 'mp3', 'duration_field': 'acapInAcapOutMainDuration'},

    # Slam dirty main
    {'file_field': 'slamDirtyMainWav_filename', 'json_field': 'slamDirtyMainWavJson', 'storage': 'wav', 'duration_field': 'slamDirtyMainDuration'},
    {'file_field': 'slamDirtyMain_filename',    'json_field': 'slamDirtyMainJson',    'storage': 'mp3', 'duration_field': 'slamDirtyMainDuration'},

    # Short acap-out
    {'file_field': 'shortAcapOutWav_filename', 'json_field': 'shortAcapOutWavJson', 'storage': 'wav', 'duration_field': 'shortAcapOutDuration'},
    {'file_field': 'shortAcapOut_filename',    'json_field': 'shortAcapOutJson',    'storage': 'mp3', 'duration_field': 'shortAcapOutDuration'},

    # Clap-in short acap-out
    {'file_field': 'clapInShortAcapOutWav_filename', 'json_field': 'clapInShortAcapOutWavJson', 'storage': 'wav', 'duration_field': 'clapInShortAcapOutDuration'},
    {'file_field': 'clapInShortAcapOut_filename',    'json_field': 'clapInShortAcapOutJson',    'storage': 'mp3', 'duration_field': 'clapInShortAcapOutDuration'},

    # Slam intro short acap-out
    {'file_field': 'slamIntroShortAcapOutWav_filename', 'json_field': 'slamIntroShortAcapOutWavJson', 'storage': 'wav', 'duration_field': 'slamIntroShortAcapOutDuration'},
    {'file_field': 'slamIntroShortAcapOut_filename',    'json_field': 'slamIntroShortAcapOutJson',    'storage': 'mp3', 'duration_field': 'slamIntroShortAcapOutDuration'},

    # Acap-in
    {'file_field': 'acapInWav_filename', 'json_field': 'acapInWavJson', 'storage': 'wav', 'duration_field': 'acapInDuration'},
    {'file_field': 'acapIn_filename',    'json_field': 'acapInJson',    'storage': 'mp3', 'duration_field': 'acapInDuration'},

    # Acap-out
    {'file_field': 'acapOutWav_filename', 'json_field': 'acapOutWavJson', 'storage': 'wav', 'duration_field': 'acapOutDuration'},
    {'file_field': 'acapOut_filename',    'json_field': 'acapOutJson',    'storage': 'mp3', 'duration_field': 'acapOutDuration'},

    # Intro
    {'file_field': 'introWav_filename', 'json_field': 'introWavJson', 'storage': 'wav', 'duration_field': 'introDuration'},
    {'file_field': 'intro_filename',    'json_field': 'introJson',    'storage': 'mp3', 'duration_field': 'introDuration'},

    # Short
    {'file_field': 'shortWav_filename', 'json_field': 'shortWavJson', 'storage': 'wav', 'duration_field': 'shortDuration'},
    {'file_field': 'short_filename',    'json_field': 'shortJson',    'storage': 'mp3', 'duration_field': 'shortDuration'},

    # Acapella
    {'file_field': 'acapellaWav_filename', 'json_field': 'acapellaWavJson', 'storage': 'wav', 'duration_field': 'acapellaDuration'},
    {'file_field': 'acapella_filename',    'json_field': 'acapellaJson',    'storage': 'mp3', 'duration_field': 'acapellaDuration'},

    # Instru
    {'file_field': 'instruWav_filename', 'json_field': 'instruWavJson', 'storage': 'wav', 'duration_field': 'instruDuration'},
    {'file_field': 'instru_filename',    'json_field': 'instruJson',    'storage': 'mp3', 'duration_field': 'instruDuration'},

    # Super short
    {'file_field': 'superShortWav_filename', 'json_field': 'superShortWavJson', 'storage': 'wav', 'duration_field': 'superShortDuration'},
    {'file_field': 'superShort_filename',    'json_field': 'superShortJson',    'storage': 'mp3', 'duration_field': 'superShortDuration'},
]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_cdn_url(filename: str, storage: str) -> str:
    encoded = requests.utils.quote(filename, safe='').replace('%20', '+')
    subfolder = 'wav' if storage == 'wav' else 'mp3'
    return f'{CDN_BASE}/tracks/{subfolder}/{encoded}'


def download_to_tmp(url: str) -> str:
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    ext = '.wav' if '/wav/' in url else '.mp3'
    fd, tmp_path = tempfile.mkstemp(suffix=ext)
    try:
        os.write(fd, resp.content)
    finally:
        os.close(fd)
    return tmp_path


def is_json_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return True
    if isinstance(value, dict) and len(value) == 0:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False


def is_waveform_key(col: str) -> bool:
    return col == 'jsonData' or col == 'previewJson' or col.endswith('Json')


def is_duration_key(col: str) -> bool:
    return col == 'duration' or col.endswith('Duration')


# Fields that are known to not exist in the current Prisma schema —
# discovered at runtime and cached so we don't retry them every track.
_missing_fields: set = set()


def safe_getattr(obj, name, default=None):
    """getattr that also treats fields we've already flagged as missing."""
    if name in _missing_fields:
        return default
    try:
        return getattr(obj, name, default)
    except Exception:
        _missing_fields.add(name)
        return default


# ─── Main ────────────────────────────────────────────────────────────────────

def main(dry_run: bool = False, limit: int | None = None):
    db = Prisma()
    db.connect()
    print("Connected to database")

    try:
        run_backfill(db, dry_run, limit)
    finally:
        db.disconnect()
        print("Disconnected from database")


def run_backfill(db, dry_run: bool, limit: int | None):
    print(f'Starting backfill: waveform + duration for {len(FILE_MAPPINGS)} file mappings...')
    if dry_run:
        print('DRY RUN — no database writes will be performed\n')
    else:
        print()

    PAGE_SIZE = 50
    offset = 0
    total_tracks = 0
    total_files_processed = 0
    total_files_skipped = 0
    total_file_errors = 0
    total_tracks_updated = 0

    while True:
        if limit is not None and total_tracks >= limit:
            break

        tracks = db.track.find_many(
            where={'isOriginal': True},
            skip=offset,
            take=PAGE_SIZE,
            order={'id': 'asc'},
        )

        if not tracks:
            break

        for track in tracks:
            total_tracks += 1
            if limit is not None and total_tracks > limit:
                break

            label = f"{track.title or '(untitled)'} [{track.id}]"
            update_data: dict = {}
            durations_captured: set = set()

            for mapping in FILE_MAPPINGS:
                ff = mapping['file_field']
                jf = mapping['json_field']
                df = mapping['duration_field']

                # Skip mappings whose fields don't exist in the generated client
                if ff in _missing_fields or jf in _missing_fields:
                    continue
                if df and df in _missing_fields:
                    df = None

                filename = safe_getattr(track, ff)
                if not filename:
                    continue

                current_json = safe_getattr(track, jf)
                current_dur = safe_getattr(track, df) if df else None

                needs_waveform = is_json_empty(current_json) and jf not in update_data
                needs_duration = (
                    df is not None
                    and (not current_dur or current_dur <= 0)
                    and df not in durations_captured
                    and df not in update_data
                )

                if not needs_waveform and not needs_duration:
                    total_files_skipped += 1
                    continue

                url = get_cdn_url(filename, mapping['storage'])
                tmp_path = None

                try:
                    reasons = []
                    if needs_waveform:
                        reasons.append('waveform')
                    if needs_duration:
                        reasons.append('duration')
                    print(f"  fetch {label} [{jf}] ({' + '.join(reasons)})...")

                    tmp_path = download_to_tmp(url)
                    result = generate_waveform(tmp_path)
                    waveform = result['waveform']
                    duration = result['duration']

                    if needs_waveform:
                        update_data[jf] = PrismaJson(waveform)
                    if needs_duration and duration > 0:
                        update_data[df] = duration
                        durations_captured.add(df)

                    total_files_processed += 1
                    print(f"  done  {label} [{jf}] => {len(waveform)} peaks, {duration:.2f}s")

                except Exception as e:
                    total_file_errors += 1
                    print(f"  ERR   {label} [{jf}]: {e}")

                finally:
                    if tmp_path:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass

                time.sleep(0.05)

            # Persist all accumulated changes for this track
            if update_data:
                if not dry_run:
                    _do_update(db, track.id, update_data, label)

                total_tracks_updated += 1
                wf_count = sum(1 for k in update_data if is_waveform_key(k))
                dur_count = sum(1 for k in update_data if is_duration_key(k))
                tag = '[DRY RUN] would save' if dry_run else 'saved'
                print(f"  {tag} {label} — {wf_count} waveform(s), {dur_count} duration(s)")

        offset += PAGE_SIZE
        print(f'\n--- processed {min(offset, total_tracks)} tracks so far ---\n')
        time.sleep(0.01)

    print('\n' + '=' * 60)
    print('SUMMARY')
    print('=' * 60)
    print(f'Tracks scanned:      {total_tracks}')
    print(f'Tracks updated:      {total_tracks_updated}')
    print(f'Files processed:     {total_files_processed}')
    print(f'Files skipped:       {total_files_skipped} (already had data)')
    print(f'File errors:         {total_file_errors}')
    if _missing_fields:
        print(f'Missing DB fields:   {sorted(_missing_fields)}')
    print('=' * 60)


def _do_update(db, track_id: str, update_data: dict, label: str):
    """
    Attempt db.track.update(); if a field is unknown to the generated Prisma
    client, strip it and retry (same strategy as database_service.py).
    """
    data = dict(update_data)
    max_retries = 5
    for attempt in range(max_retries):
        try:
            db.track.update(where={'id': track_id}, data=data)
            return
        except Exception as e:
            err_str = str(e)
            if 'Could not find field' in err_str:
                for field_name in list(data.keys()):
                    if field_name in err_str:
                        print(f"  WARN  Field '{field_name}' not in Prisma client — skipping (run 'prisma generate' to fix)")
                        _missing_fields.add(field_name)
                        data.pop(field_name, None)
                        break
                else:
                    raise
                if not data:
                    return
            else:
                raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Backfill waveforms and durations for all track variants'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be updated without writing to DB',
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='Max number of tracks to process',
    )
    args = parser.parse_args()

    try:
        main(dry_run=args.dry_run, limit=args.limit)
    except KeyboardInterrupt:
        print('\nInterrupted by user')
        sys.exit(130)
    except Exception as e:
        print(f'Fatal error: {e}')
        traceback.print_exc()
        sys.exit(1)
