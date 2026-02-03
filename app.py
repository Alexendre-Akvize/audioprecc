import os

# Load environment variables from .env file FIRST (before any os.environ.get calls)
from dotenv import load_dotenv
load_dotenv()

import subprocess
import threading
import shutil
import time
import re
import zipfile
import io
import csv
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory, abort, send_file, session, Response
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3, TIT2, TPE1, APIC, TALB, TDRC, TRCK, TCON, TBPM, TSRC, TLEN, TPUB, WOAR, WXXX, TXXX
from pydub import AudioSegment
import librosa
import numpy as np
import scipy.io.wavfile as wavfile
import urllib.parse

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'idbyrivoli-secret-key-2024')

# Configure Flask for large batch uploads (1000+ tracks)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max per request
app.config['MAX_FORM_MEMORY_SIZE'] = 100 * 1024 * 1024  # 100MB form memory

# Upload concurrency control - limit simultaneous uploads to prevent server overload
UPLOAD_SEMAPHORE = threading.Semaphore(int(os.environ.get('MAX_CONCURRENT_UPLOADS', 50)))

# Use absolute paths to avoid confusion
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'output')
PROCESSED_FOLDER = os.path.join(BASE_DIR, 'processed')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Multi-user session support
import uuid
from threading import Lock

# Dictionary to store job status per session
sessions_status = {}
sessions_lock = Lock()

# Track downloads - files stay until track.idbyrivoli.com confirms successful download
# Structure: { "Track Name": {"files_total": 6, "original_path": "/path/to/original.mp3", "created_at": timestamp, "files": {...}} }
download_tracker = {}
download_tracker_lock = Lock()

# Pending downloads awaiting confirmation from track.idbyrivoli.com
# Structure: { "Track Name": {"created_at": timestamp, "files": [], "processed_dir": "...", "original_path": "...", "htdemucs_dir": "..."} }
pending_downloads = {}
pending_downloads_lock = Lock()

# Scheduled deletions - tracks confirmed for download, waiting 5 hours before deletion
# Structure: { "Track Name": {"scheduled_at": timestamp, "delete_after": timestamp} }
scheduled_deletions = {}
scheduled_deletions_lock = Lock()
DELETION_DELAY_MINUTES = int(os.environ.get('DELETION_DELAY_MINUTES', 300))  # 5 hours (300 minutes) delay after confirm_download

# Maximum number of pending tracks before warning (configurable)
# Set high to support batch uploads of 1000+ tracks
MAX_PENDING_TRACKS = int(os.environ.get('MAX_PENDING_TRACKS', 1500))
PENDING_WARNING_THRESHOLD = int(os.environ.get('PENDING_WARNING_THRESHOLD', 1000))

# =============================================================================
# DROPBOX INTEGRATION
# =============================================================================
# Set DROPBOX_ACCESS_TOKEN in your .env file
# Get your token from: https://www.dropbox.com/developers/apps
# For Dropbox Business team tokens, also set DROPBOX_TEAM_MEMBER_ID
DROPBOX_ACCESS_TOKEN = os.environ.get('DROPBOX_ACCESS_TOKEN', '')
DROPBOX_TEAM_MEMBER_ID = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '')
DROPBOX_FOLDER = os.path.join(BASE_DIR, 'dropbox_downloads')
os.makedirs(DROPBOX_FOLDER, exist_ok=True)

# Dropbox import tracking
dropbox_imports = {}  # { import_id: { status, total, downloaded, processed, files, errors } }
dropbox_imports_lock = Lock()

# =============================================================================
# PERSISTENT BULK IMPORT - Runs in background even if browser closes
# =============================================================================
bulk_import_state = {
    'active': False,
    'stop_requested': False,
    'folder_path': '',
    'namespace_id': '',
    'started_at': None,
    'total_found': 0,
    'total_scanned': 0,
    'downloaded': 0,
    'processed': 0,
    'failed': 0,
    'skipped': 0,  # Tracks skipped due to banned keywords
    'current_file': '',
    'current_status': 'idle',  # idle, scanning, downloading, processing, complete, stopped, error
    'files_queue': [],  # Files waiting to be processed
    'completed_files': [],  # Successfully processed files
    'failed_files': [],  # Failed files with error messages
    'skipped_files': [],  # Skipped files with reasons
    'error': None,
    'last_update': None
}
bulk_import_lock = Lock()

# =============================================================================
# SEQUENTIAL PROCESSING MODE - Track individual file downloads
# =============================================================================
# Structure: { "Track Name": {"files": {"filename.mp3": False, "filename.wav": True, ...}, "all_downloaded": False} }
# Each file is marked True when downloaded, track is "completed" only when all files downloaded
track_download_status = {}
track_download_status_lock = Lock()

# Current track being processed in sequential mode
current_processing_track = None
current_processing_track_lock = Lock()

# Sequential mode flag - when True, process one track at a time and wait for downloads
SEQUENTIAL_MODE = True  # Enabled by default for the new workflow

def register_track_files(track_name, file_list):
    """Register all files for a track that need to be downloaded."""
    with track_download_status_lock:
        track_download_status[track_name] = {
            'files': {f: False for f in file_list},
            'all_downloaded': False,
            'created_at': time.time()
        }
        print(f"📋 Registered {len(file_list)} files for download: {track_name}")
        for f in file_list:
            print(f"   - {f}")

def mark_file_downloaded(track_name, filename):
    """Mark a specific file as downloaded. Returns True if all files for track are now downloaded."""
    with track_download_status_lock:
        if track_name not in track_download_status:
            print(f"⚠️ Track '{track_name}' not found in download status")
            return False
        
        # Find the file (may need to match partial name)
        files = track_download_status[track_name]['files']
        matched = False
        for f in files:
            if filename in f or f in filename or os.path.basename(f) == os.path.basename(filename):
                files[f] = True
                matched = True
                print(f"✅ Marked as downloaded: {f}")
                break
        
        if not matched:
            print(f"⚠️ File '{filename}' not found in track '{track_name}'")
            return False
        
        # Check if all files are downloaded
        all_downloaded = all(files.values())
        track_download_status[track_name]['all_downloaded'] = all_downloaded
        
        if all_downloaded:
            print(f"🎉 All files downloaded for: {track_name}")
        else:
            remaining = sum(1 for v in files.values() if not v)
            print(f"📥 {remaining} files remaining for: {track_name}")
        
        return all_downloaded

def get_track_download_status(track_name):
    """Get download status for a specific track."""
    with track_download_status_lock:
        if track_name in track_download_status:
            return track_download_status[track_name].copy()
        return None

def is_track_fully_downloaded(track_name):
    """Check if all files for a track have been downloaded."""
    with track_download_status_lock:
        if track_name not in track_download_status:
            return False
        return track_download_status[track_name]['all_downloaded']

def get_pending_downloads_for_track(track_name):
    """Get list of files not yet downloaded for a track."""
    with track_download_status_lock:
        if track_name not in track_download_status:
            return []
        files = track_download_status[track_name]['files']
        return [f for f, downloaded in files.items() if not downloaded]

def cleanup_track_after_downloads(track_name):
    """Delete track files after all versions have been downloaded."""
    with track_download_status_lock:
        if track_name in track_download_status:
            del track_download_status[track_name]
    
    # Trigger the actual file deletion
    confirm_track_download(track_name)
    print(f"🗑️ Cleaned up track after full download: {track_name}")

def get_pending_tracks_count():
    """Get the number of tracks pending download confirmation."""
    with pending_downloads_lock:
        return len(pending_downloads)

def check_pending_tracks_warning():
    """Check if there are too many pending tracks and return warning message if so."""
    count = get_pending_tracks_count()
    if count >= MAX_PENDING_TRACKS:
        return {
            'warning': True,
            'level': 'critical',
            'message': f'⚠️ CRITICAL: {count} tracks en attente de téléchargement (max: {MAX_PENDING_TRACKS}). Veuillez télécharger les tracks existants avant d\'en ajouter de nouveaux.',
            'count': count,
            'max': MAX_PENDING_TRACKS
        }
    elif count >= PENDING_WARNING_THRESHOLD:
        return {
            'warning': True,
            'level': 'warning',
            'message': f'⚠️ {count} tracks en attente de téléchargement. Pensez à télécharger les tracks existants.',
            'count': count,
            'threshold': PENDING_WARNING_THRESHOLD
        }
    return {'warning': False, 'count': count}

def track_file_for_pending_download(track_name, original_path, num_files=6, file_list=None):
    """
    Register a track as pending download - files will stay until track.idbyrivoli.com confirms download.
    If file_list is provided, also register for sequential download tracking.
    """
    with pending_downloads_lock:
        # Also track the htdemucs intermediate folder
        htdemucs_dir = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
        pending_downloads[track_name] = {
            'files_total': num_files,
            'files': file_list or [],  # Will be populated with file paths
            'original_path': original_path,
            'processed_dir': os.path.join(PROCESSED_FOLDER, track_name),
            'htdemucs_dir': htdemucs_dir,
            'created_at': time.time()
        }
        
        pending_count = len(pending_downloads)
        print(f"")
        print(f"📝 ════════════════════════════════════════════════")
        print(f"📝 PENDING: Registered '{track_name}'")
        print(f"📝 Files available: {num_files}")
        print(f"📝 Original: {original_path}")
        print(f"📝 Processed dir: {PROCESSED_FOLDER}/{track_name}")
        print(f"📝 Status: AWAITING download")
        print(f"📝 Total pending tracks: {pending_count}")
        if pending_count >= PENDING_WARNING_THRESHOLD:
            print(f"📝 ⚠️ WARNING: {pending_count} tracks pending (threshold: {PENDING_WARNING_THRESHOLD})")
        print(f"📝 ════════════════════════════════════════════════")
    
    # SEQUENTIAL MODE: Also register for individual file download tracking
    if SEQUENTIAL_MODE and file_list:
        register_track_files(track_name, file_list)

def schedule_track_deletion(track_name):
    """
    Schedule a track for deletion after DELETION_DELAY_MINUTES.
    Called when track.idbyrivoli.com confirms successful download.
    Returns True if track was found and scheduled, False otherwise.
    """
    with pending_downloads_lock:
        if track_name not in pending_downloads:
            print(f"⚠️ Track '{track_name}' not found in pending downloads")
            return False
        
        track_info = pending_downloads[track_name].copy()
    
    # Schedule for deletion
    now = time.time()
    delete_after = now + (DELETION_DELAY_MINUTES * 60)
    
    with scheduled_deletions_lock:
        scheduled_deletions[track_name] = {
            'scheduled_at': now,
            'delete_after': delete_after,
            'track_info': track_info
        }
    
    # Remove from pending downloads (it's now scheduled for deletion)
    with pending_downloads_lock:
        if track_name in pending_downloads:
            del pending_downloads[track_name]
    
    print(f"")
    print(f"⏰ ════════════════════════════════════════════════")
    print(f"⏰ SCHEDULED FOR DELETION: '{track_name}'")
    print(f"⏰ Will be deleted in {DELETION_DELAY_MINUTES} minutes")
    print(f"⏰ Delete after: {time.strftime('%H:%M:%S', time.localtime(delete_after))}")
    print(f"⏰ ════════════════════════════════════════════════")
    
    return True

def confirm_track_download(track_name, add_to_logs=True):
    """
    Actually delete all files associated with this track.
    Called by the scheduled deletion thread after the delay.
    Returns True if track was found and deleted, False otherwise.
    """
    # Get track info from scheduled deletions
    with scheduled_deletions_lock:
        if track_name in scheduled_deletions:
            track_info = scheduled_deletions[track_name].get('track_info', {})
        else:
            # Fallback to pending downloads for backward compatibility
            with pending_downloads_lock:
                if track_name not in pending_downloads:
                    print(f"⚠️ Track '{track_name}' not found")
                    return False
                track_info = pending_downloads[track_name]
    
    print(f"")
    print(f"✅ ════════════════════════════════════════════════")
    print(f"✅ DELETING TRACK: '{track_name}'")
    print(f"✅ Cleaning up files...")
    
    # Delete processed folder
    if track_info.get('processed_dir') and os.path.exists(track_info['processed_dir']):
        try:
            shutil.rmtree(track_info['processed_dir'])
            print(f"   🗑️ Deleted processed folder: {track_info['processed_dir']}")
        except Exception as e:
            print(f"   ⚠️ Could not delete processed folder: {e}")
    
    # Delete original upload file
    if track_info.get('original_path') and os.path.exists(track_info['original_path']):
        try:
            os.remove(track_info['original_path'])
            print(f"   🗑️ Deleted original: {track_info['original_path']}")
        except Exception as e:
            print(f"   ⚠️ Could not delete original: {e}")
    
    # Delete htdemucs intermediate folder
    if track_info.get('htdemucs_dir') and os.path.exists(track_info['htdemucs_dir']):
        try:
            shutil.rmtree(track_info['htdemucs_dir'])
            print(f"   🗑️ Deleted htdemucs folder: {track_info['htdemucs_dir']}")
        except Exception as e:
            print(f"   ⚠️ Could not delete htdemucs folder: {e}")
    
    # Remove from scheduled deletions
    with scheduled_deletions_lock:
        if track_name in scheduled_deletions:
            del scheduled_deletions[track_name]
    
    # Also remove from pending downloads if still there
    with pending_downloads_lock:
        if track_name in pending_downloads:
            del pending_downloads[track_name]
    
    print(f"   ✅ Cleanup complete for '{track_name}'")
    print(f"✅ ════════════════════════════════════════════════")
    
    # Add to frontend logs
    if add_to_logs:
        try:
            log_message(f"🗑️ Fichiers supprimés: {track_name}")
        except:
            pass  # log_message might not be defined yet during startup
    
    return True

def process_scheduled_deletions():
    """
    Background thread that processes scheduled deletions.
    Deletes tracks that have passed their deletion delay.
    """
    while True:
        try:
            time.sleep(30)  # Check every 30 seconds
            
            now = time.time()
            tracks_to_delete = []
            
            with scheduled_deletions_lock:
                for track_name, info in scheduled_deletions.items():
                    if now >= info['delete_after']:
                        tracks_to_delete.append(track_name)
            
            for track_name in tracks_to_delete:
                print(f"⏰ Deletion delay expired for '{track_name}', deleting now...")
                confirm_track_download(track_name)
                
        except Exception as e:
            print(f"⚠️ Scheduled deletion error: {e}")

# Start scheduled deletion thread
scheduled_deletion_thread = threading.Thread(target=process_scheduled_deletions, daemon=True)
scheduled_deletion_thread.start()

def get_pending_tracks_list():
    """Get list of all pending tracks with their info."""
    with pending_downloads_lock:
        tracks = []
        now = time.time()
        for track_name, info in pending_downloads.items():
            age_hours = (now - info.get('created_at', now)) / 3600
            tracks.append({
                'track_name': track_name,
                'files_total': info.get('files_total', 0),
                'created_at': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(info.get('created_at', now))),
                'age_hours': round(age_hours, 2),
                'processed_dir': info.get('processed_dir', ''),
            })
        # Sort by creation time (oldest first)
        tracks.sort(key=lambda x: x['age_hours'], reverse=True)
        return tracks

print("📥 Pending downloads system initialized (files stay until API confirms download)")

# =============================================================================
# UPLOAD HISTORY TRACKING - CSV Export feature
# =============================================================================
# Structure: { "filename": {"status": str, "date": str, "type": str, "session_id": str, "error": str|None} }
upload_history = {}
upload_history_lock = Lock()
HISTORY_FILE = os.path.join(BASE_DIR, 'upload_history.csv')

def detect_track_type_from_title(title):
    """
    Detect if track title contains Instrumental, Extended, or Acapella.
    Returns the detected type or None if regular track.
    Case-insensitive matching.
    """
    if not title:
        return None
    
    title_lower = title.lower()
    
    # Check for specific keywords
    if 'instrumental' in title_lower:
        return 'Instrumental'
    elif 'acapella' in title_lower or 'a capella' in title_lower or 'acappella' in title_lower:
        return 'Acapella'
    elif 'extended' in title_lower:
        return 'Extended'
    
    return None


# =============================================================================
# TITLE CLEANING AND FILTERING SYSTEM
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
}

# Track types/versions to generate
TRACK_VERSIONS = ['Dirty', 'Clean']


def should_skip_track(title):
    """
    Check if track should be skipped based on keywords in title.
    Returns (should_skip: bool, reason: str or None)
    """
    if not title:
        return False, None
    
    title_lower = title.lower()
    
    for keyword in SKIP_KEYWORDS:
        if keyword.lower() in title_lower:
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
    
    # 2. Remove BPM numbers at the end (standalone numbers like "102", "128", etc.)
    # Match: space + 2-3 digit number at end, or space + number + space before parenthesis
    cleaned = re.sub(r'\s+\d{2,3}\s*$', '', cleaned)  # "Title 102" -> "Title"
    cleaned = re.sub(r'\s+\d{2,3}\s+(\([^)]+\))\s*$', r' \1', cleaned)  # "Title 102 (Intro)" -> "Title (Intro)"
    
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


# Log the configuration
print(f"🏷️  Title Cleaning: {len(SKIP_KEYWORDS)} skip keywords, {len(DJ_NAMES_TO_REPLACE)} DJ names to replace")

def add_to_upload_history(filename, session_id, status='uploaded', track_type='Unknown', error=None):
    """Add or update a file in the upload history."""
    with upload_history_lock:
        upload_history[filename] = {
            'filename': filename,
            'status': status,
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'type': track_type,
            'session_id': session_id,
            'error': error
        }
        # Save to CSV file for persistence
        save_history_to_csv()

def update_upload_history_status(filename, status, track_type=None, error=None):
    """Update the status of a file in the upload history."""
    with upload_history_lock:
        if filename in upload_history:
            upload_history[filename]['status'] = status
            upload_history[filename]['date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if track_type:
                upload_history[filename]['type'] = track_type
            if error:
                upload_history[filename]['error'] = error
            save_history_to_csv()

def save_history_to_csv():
    """Save upload history to CSV file (called within lock)."""
    try:
        with open(HISTORY_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['filename', 'status', 'date', 'type', 'session_id', 'error'])
            writer.writeheader()
            for entry in upload_history.values():
                writer.writerow(entry)
    except Exception as e:
        print(f"⚠️ Could not save history to CSV: {e}")

def load_history_from_csv():
    """Load upload history from CSV file on startup."""
    global upload_history
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    upload_history[row['filename']] = row
            print(f"📋 Loaded {len(upload_history)} entries from upload history")
        except Exception as e:
            print(f"⚠️ Could not load history from CSV: {e}")

def get_upload_history_list():
    """Get the upload history as a sorted list (newest first)."""
    with upload_history_lock:
        entries = list(upload_history.values())
        # Sort by date descending
        entries.sort(key=lambda x: x['date'], reverse=True)
        return entries

def clear_upload_history():
    """Clear all upload history."""
    global upload_history
    with upload_history_lock:
        upload_history = {}
        if os.path.exists(HISTORY_FILE):
            os.remove(HISTORY_FILE)

# Load history on startup
load_history_from_csv()
print(f"📋 Upload history system initialized ({len(upload_history)} entries)")

def log_file_download(track_name, filepath):
    """
    Log when a file is downloaded (for monitoring purposes).
    Files are NOT deleted here - they stay until track.idbyrivoli.com confirms download.
    """
    file_basename = os.path.basename(filepath)
    print(f"📥 File downloaded for '{track_name}': {file_basename}")
    print(f"   ℹ️ File will remain available until download is confirmed via /confirm_download")

def get_session_id():
    """Get or create a unique session ID for the current user."""
    from flask import session
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())[:8]
    return session['session_id']

def get_job_status(session_id=None):
    """Get job status for a specific session."""
    if session_id is None:
        session_id = 'global'
    
    with sessions_lock:
        if session_id not in sessions_status:
            sessions_status[session_id] = {
                'state': 'idle', 
                'progress': 0,
                'total_files': 0,
                'current_file_idx': 0,
                'current_filename': '',
                'current_step': '',
                'results': [],
                'error': None,
                'logs': [],
                'session_id': session_id,
                'failed_files': [],  # Track files that failed processing
                'retry_count': {}    # Track retry attempts per file
            }
        # Ensure existing sessions have the new fields
        if 'failed_files' not in sessions_status[session_id]:
            sessions_status[session_id]['failed_files'] = []
        if 'retry_count' not in sessions_status[session_id]:
            sessions_status[session_id]['retry_count'] = {}
        return sessions_status[session_id]

# Global variable for backward compatibility
job_status = get_job_status('global')

def log_message(message, session_id=None):
    """Adds a message to the job logs and prints it."""
    print(message)
    timestamp = time.strftime("%H:%M:%S")
    
    # Log to specific session if provided
    if session_id:
        status = get_job_status(session_id)
        status['logs'].append(f"[{timestamp}] {message}")
        if len(status['logs']) > 1000:
            status['logs'] = status['logs'][-1000:]
    
    # Also log to global for backward compatibility
    job_status['logs'].append(f"[{timestamp}] {message}")
    if len(job_status['logs']) > 1000:
        job_status['logs'] = job_status['logs'][-1000:]

@app.route('/download_all_zip')
def download_all_zip():
    """
    Creates a ZIP file containing all processed tracks and sends it to the user.
    Can be called at any time to get currently finished tracks.
    """
    global job_status
    
    # Refresh results from disk if needed
    if not job_status['results']:
        # ... (logic to populate from disk, similar to status route)
        processed_dirs = [d for d in os.listdir(PROCESSED_FOLDER) if os.path.isdir(os.path.join(PROCESSED_FOLDER, d))]
        # We need to rebuild job_status['results'] or just iterate dirs directly
        pass 

    # Create an in-memory ZIP file
    memory_file = io.BytesIO()
    
    # We zip everything currently in PROCESSED_FOLDER
    has_files = False
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(PROCESSED_FOLDER):
             for file in files:
                if file.lower().endswith(('.mp3', '.wav')): 
                    file_path = os.path.join(root, file)
                    # Create relative path inside zip: "Track Name/Track Name Main.mp3"
                    rel_path = os.path.relpath(file_path, PROCESSED_FOLDER)
                    zf.write(file_path, rel_path)
                    has_files = True

    if not has_files:
        return jsonify({'error': 'Aucun fichier traité disponible pour le moment'}), 400

    memory_file.seek(0)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f'ID_By_Rivoli_Pack_{timestamp}.zip'
    )

def clean_filename(filename):
    """
    Cleans filename: removes underscores, specific patterns, and unnecessary IDs.
    Example: DJ_Mustard_ft.Travis_Scott-Whole_Lotta_Lovin_Edits_and_Intro_Outros-Radio_Edit-77055446
    Result: DJ Mustard ft. Travis Scott - Whole Lotta Lovin Edits and Intro Outros
    """
    name, ext = os.path.splitext(filename)
    name = name.replace('_', ' ')
    name = re.sub(r'-\d+$', '', name)
    name = re.sub(r'\.(?=[A-Z])', '. ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name, ext

def format_artists(artist_string):
    """
    Formats multiple artists with proper separators.
    - 2 artists: "Artist A & Artist B"
    - 3+ artists: "Artist A, Artist B, Artist C & Artist D"
    
    Handles various input separators: /, ;, feat., ft., and, &
    Ensures proper ASCII output (no unicode escapes like \u0026)
    """
    if not artist_string:
        return artist_string
    
    # Convert to string and decode any unicode escapes
    normalized = str(artist_string)
    
    # Decode unicode escapes if present (e.g., \u0026 -> &)
    try:
        if '\\u' in normalized:
            normalized = normalized.encode().decode('unicode_escape')
    except:
        pass
    
    # Replace null character \u0000 (common ID3 multi-value separator) with our separator
    normalized = normalized.replace('\x00', '|||')
    
    # Normalize separators - replace common separators with a standard one
    # Replace "feat.", "ft.", "Feat.", "Ft." with separator
    normalized = re.sub(r'\s*(?:feat\.?|ft\.?|Feat\.?|Ft\.?)\s*', '|||', normalized, flags=re.IGNORECASE)
    # Replace "|", " / ", "/", " & ", " and ", ";" with separator
    normalized = re.sub(r'\s*\|\s*', '|||', normalized)  # Pipe separator
    normalized = re.sub(r'\s*/\s*', '|||', normalized)
    normalized = re.sub(r'\s*;\s*', '|||', normalized)
    normalized = re.sub(r'\s+&\s+', '|||', normalized)
    normalized = re.sub(r'\s+and\s+', '|||', normalized, flags=re.IGNORECASE)
    
    # Split by our separator
    artists = [a.strip() for a in normalized.split('|||') if a.strip()]
    
    if len(artists) == 0:
        return artist_string
    elif len(artists) == 1:
        return artists[0]
    elif len(artists) == 2:
        return f"{artists[0]} & {artists[1]}"
    else:
        # 3 or more: "A, B, C & D"
        return ', '.join(artists[:-1]) + ' & ' + artists[-1]

# Label mapping: sub-labels to parent labels
LABEL_MAPPINGS = {
    'Universal Music Group': [
        'PolyGram Music Publishing',
        'Rondor Music',
        'Edition Ricordi',
        'Decca Publishing',
        'Universal Music Publishing Classical',
        'Universal Music Publishing Production Music',
        'Universal Music Publishing France',
        'Universal Music Publishing UK',
        'Universal Music Publishing US',
        'Universal Music Publishing Germany',
        'Universal Music Publishing Benelux',
        'Universal Music Publishing Scandinavia',
        'Universal Music Publishing Latin America',
        'Universal Music Publishing Asia',
        'Eagle-i Music',
        'Global Dog Publishing',
        'Casablanca Media Publishing',
        'Criterion Music Corp',
        'Beechwood Music Corp',
        'Universal Songs of PolyGram International',
        'Abbey Road Masters',
        'Island Music Publishing',
        'Motown Music Publishing',
        'Def Jam Music Publishing',
        'Capitol Music Publishing',
    ],
    'Sony Music Group': [
        'EMI Music Publishing',
        'ATV Music Publishing',
        'Famous Music',
        'Jobete Music',
        'Sony/ATV Latin',
        'Sony/ATV Europe',
        'Sony/ATV Scandinavia',
        'Sony/ATV France',
        'Sony/ATV Germany',
        'Sony/ATV UK',
        'Sony/ATV US',
        'Sony Music Publishing Production Music',
        'Extreme Music',
        'Tree Publishing',
        'Firstcom Music',
        'Filmtrax',
        'EMI Production Music',
        'Motown Catalog Publishing',
        'Chrysalis Songs',
    ],
    'Warner Music Group': [
        'Chappell & Co',
        'Warner Chappell Production Music',
        'Blue Mountain Music',
        'CPP',
        'Copyright Protection Provider',
        'Warner Chappell France',
        'Warner Chappell UK',
        'Warner Chappell US',
        'Warner Chappell Germany',
        'Warner Chappell Benelux',
        'Warner Chappell Scandinavia',
        'Warner Chappell Latin America',
        'Warner Chappell Asia',
        # Ex-Alfred Music Publishing (now part of Warner)
        'Alfred Music Publishing',
        'Faber Music',
        'Imagem Music Group',
        'Boosey & Hawkes',
        'Birch Tree Music',
        'Non-Stop Music',
        'Music Sales Group',
    ],
}

def get_parent_label(sub_label):
    """
    Maps a sub-label to its parent label.
    Returns the parent label if matched, otherwise returns the original sub-label.
    Case-insensitive matching with flexibility for partial matches.
    """
    if not sub_label:
        return ''
    
    sub_label_clean = sub_label.strip()
    sub_label_lower = sub_label_clean.lower()
    
    for parent_label, sub_labels in LABEL_MAPPINGS.items():
        for known_sub in sub_labels:
            known_sub_lower = known_sub.lower()
            # Exact match (case-insensitive)
            if sub_label_lower == known_sub_lower:
                return parent_label
            # Partial match: sub_label contains known_sub or vice versa
            # Be careful to avoid false positives - require substantial match
            if len(known_sub_lower) >= 5:  # Only for longer names to avoid false positives
                if known_sub_lower in sub_label_lower or sub_label_lower in known_sub_lower:
                    # Additional check: at least 70% of characters match
                    shorter = min(len(known_sub_lower), len(sub_label_lower))
                    if shorter >= 5:
                        return parent_label
    
    # No match found, return original
    return sub_label_clean

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

import requests
from datetime import datetime

# API Endpoint Configuration
API_ENDPOINT = os.environ.get('API_ENDPOINT', 'https://track.idbyrivoli.com/upload')
API_KEY = os.environ.get('API_KEY', '5X#JP5ifkSm?oE6@haMriYG$j!87BEfX@zg3CxcE')

# =============================================================================
# DATABASE MODE CONFIGURATION
# =============================================================================
# When USE_DATABASE_MODE is True, tracks are created directly in the database
# instead of calling the external API. This provides better performance and
# eliminates dependency on the external track service.
#
# Required environment variables for database mode:
#   DATABASE_URL or (DATABASE_HOST, DATABASE_PORT, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
#
# Set USE_DATABASE_MODE=true in environment to enable
# =============================================================================
USE_DATABASE_MODE = os.environ.get('USE_DATABASE_MODE', 'true').lower() in ('true', '1', 'yes')

# Initialize database service if in database mode
_database_service = None
if USE_DATABASE_MODE:
    try:
        from database_service import get_database_service, save_track_to_database, check_database_connection
        _database_service = get_database_service()
        if check_database_connection():
            print("✅ Database mode enabled - tracks will be saved directly to database")
        else:
            print("⚠️ Database mode enabled but connection failed - falling back to API mode")
            USE_DATABASE_MODE = False
    except ImportError as e:
        print(f"⚠️ Database service not available: {e}")
        USE_DATABASE_MODE = False
    except Exception as e:
        print(f"⚠️ Database initialization failed: {e}")
        USE_DATABASE_MODE = False

if not USE_DATABASE_MODE:
    print("📡 API mode enabled - tracks will be sent to external API")

# Dynamic Public URL handling
CURRENT_HOST_URL = os.environ.get('PUBLIC_URL', '')

@app.before_request
def set_public_url():
    """Captures the current public URL from the request headers to support dynamic Pod URLs (RunPod, etc.)."""
    global CURRENT_HOST_URL
    
    # Always try to get the best URL from headers on each request
    # Priority: X-Forwarded-Host > Host header > existing value
    forwarded_host = request.headers.get('X-Forwarded-Host')
    original_host = request.headers.get('Host')
    # Default to http since this server typically runs without SSL
    scheme = request.headers.get('X-Forwarded-Proto', 'http')
    
    # Debug: log all relevant headers on first request
    if not CURRENT_HOST_URL or 'localhost' in CURRENT_HOST_URL:
        print(f"🔍 Headers debug:")
        print(f"   X-Forwarded-Host: {forwarded_host}")
        print(f"   X-Forwarded-Proto: {scheme}")
        print(f"   Host: {original_host}")
        print(f"   X-Real-IP: {request.headers.get('X-Real-IP')}")
        print(f"   Origin: {request.headers.get('Origin')}")
        print(f"   Referer: {request.headers.get('Referer')}")
    
    new_url = None
    
    # RunPod and similar platforms set X-Forwarded-Host to the public URL
    if forwarded_host and 'localhost' not in forwarded_host:
        new_url = f"{scheme}://{forwarded_host}"
    # Try Origin header (set by browser on CORS requests)
    elif request.headers.get('Origin') and 'localhost' not in request.headers.get('Origin', ''):
        new_url = request.headers.get('Origin')
    # Try Referer header
    elif request.headers.get('Referer') and 'localhost' not in request.headers.get('Referer', ''):
        # Extract base URL from referer
        referer = request.headers.get('Referer')
        from urllib.parse import urlparse
        parsed = urlparse(referer)
        if parsed.netloc and 'localhost' not in parsed.netloc:
            new_url = f"{parsed.scheme}://{parsed.netloc}"
    # Use Host header only if it's not a private IP or localhost
    elif original_host and not original_host.startswith(('10.', '172.', '192.168.', '100.', 'localhost', '127.')):
        new_url = f"{scheme}://{original_host}"
    
    # Update if we found a valid public URL (not localhost)
    if new_url and 'localhost' not in new_url and new_url != CURRENT_HOST_URL:
        CURRENT_HOST_URL = new_url
        print(f"📍 Public URL détectée: {CURRENT_HOST_URL}")

def send_track_info_to_api(track_data):
    """
    Sends track information to external API endpoint with authentication,
    OR saves directly to database if USE_DATABASE_MODE is enabled.
    """
    import json
    
    # Log the payload being processed
    print(f"\n{'='*60}")
    print(f"📤 TRACK DATA for: {track_data.get('Titre', 'N/A')} ({track_data.get('Format', 'N/A')})")
    print(f"{'='*60}")
    print(json.dumps(track_data, indent=2, ensure_ascii=False))
    print(f"{'='*60}\n")
    
    # Use database mode if enabled
    if USE_DATABASE_MODE:
        try:
            result = save_track_to_database(track_data)
            
            if 'error' in result:
                print(f"❌ DATABASE ERROR: {result['error']}")
                log_message(f"DB ERROR: {track_data['Titre']} - {result['error']}")
            else:
                action = result.get('action', 'saved')
                print(f"✅ DATABASE SUCCESS: {track_data['Titre']} ({track_data['Format']}) [{action}]")
                log_message(f"DB OK: {track_data['Titre']} ({track_data['Format']}) → {result.get('id', 'N/A')} [{action}]")
            
            return result
            
        except Exception as e:
            print(f"❌ DATABASE EXCEPTION: {e}")
            log_message(f"DB EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    # Fall back to API mode
    if not API_ENDPOINT:
        print("⚠️  API_ENDPOINT not configured, skipping API call")
        return
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {API_KEY}'
        }
        
        response = requests.post(API_ENDPOINT, json=track_data, headers=headers, timeout=30)
        
        if response.status_code in [200, 202]:
            print(f"✅ API SUCCESS: {track_data['Titre']} ({track_data['Format']})")
            log_message(f"API OK: {track_data['Titre']} ({track_data['Format']}) → {track_data.get('Fichiers', '')}")
        else:
            print(f"❌ API ERROR {response.status_code}: {response.text[:200]}")
            log_message(f"API ERROR {response.status_code} pour {track_data['Titre']}")
            
    except Exception as e:
        print(f"❌ API EXCEPTION: {e}")
        log_message(f"API EXCEPTION: {e}")

def prepare_track_metadata(edit_info, original_path, bpm, base_url=""):
    """
    Prepares track metadata for API export with absolute URLs.
    """
    global CURRENT_HOST_URL
    
    # Fallback if request hasn't set it yet
    base_url = CURRENT_HOST_URL if CURRENT_HOST_URL else ""
    
    # Warn if we don't have a valid public URL
    if not base_url or 'localhost' in base_url:
        print(f"⚠️ WARNING: No valid public URL detected! API calls may fail.")
        print(f"   Current CURRENT_HOST_URL: {CURRENT_HOST_URL}")
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
        
        # Extract original cover (Cover 2) from source file and use that URL
        cover_url = f"{base_url}/static/covers/Cover_Id_by_Rivoli.jpeg"  # Fallback only
        original_cover_found = False
        
        # Try to extract original cover from source file
        if original_tags:
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
                        print(f"   ✅ Cover originale extraite: {cover_filename}")
                        break
                    except Exception as e:
                        print(f"   ❌ Could not extract cover from {apic_key}: {e}")
        
        if not original_cover_found:
            print(f"   ⚠️ Pas de cover originale trouvée, utilisation cover ID By Rivoli")
        
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
            'TRACK_ID': track_id
        }
        
        return track_data
        
    except Exception as e:
        print(f"Error preparing track metadata: {e}")
        return None

import audio_processor

# Detect if GPU is available for Demucs acceleration
def get_demucs_device(force_check=False):
    """Detect best device for Demucs (CUDA GPU or CPU)."""
    try:
        import torch
        
        # Force CUDA initialization
        if torch.cuda.is_available():
            # Try to actually use CUDA to make sure it works
            try:
                torch.cuda.init()
                device_count = torch.cuda.device_count()
                if device_count > 0:
                    gpu_name = torch.cuda.get_device_name(0)
                    gpu_mem = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                    print(f"🚀 GPU détecté: {gpu_name} ({gpu_mem:.0f}GB) - Mode CUDA activé")
                    print(f"   CUDA version: {torch.version.cuda}")
                    print(f"   PyTorch version: {torch.__version__}")
                    return 'cuda'
            except Exception as e:
                print(f"⚠️ CUDA disponible mais erreur d'init: {e}")
        else:
            print(f"⚠️ torch.cuda.is_available() = False")
            print(f"   PyTorch version: {torch.__version__}")
            print(f"   CUDA built: {torch.backends.cuda.is_built() if hasattr(torch.backends, 'cuda') else 'N/A'}")
    except Exception as e:
        print(f"❌ Erreur détection GPU: {e}")
    
    print("💻 Mode CPU activé")
    return 'cpu'

# Detect device at startup
DEMUCS_DEVICE = get_demucs_device()

def ensure_cuda_device():
    """Re-check CUDA availability (call before processing)."""
    global DEMUCS_DEVICE
    if DEMUCS_DEVICE == 'cpu':
        # Try again in case CUDA wasn't ready at import time
        DEMUCS_DEVICE = get_demucs_device(force_check=True)
    return DEMUCS_DEVICE

def create_edits(vocals_path, inst_path, original_path, base_output_path, base_filename):
    print(f"Loading audio for edits: {base_filename}")
    
    # Get BPM from original file metadata (don't auto-detect)
    bpm = None
    try:
        original_audio = MP3(original_path, ID3=ID3)
        if original_audio.tags and 'TBPM' in original_audio.tags:
            bpm_text = str(original_audio.tags['TBPM'].text[0]).strip()
            if bpm_text:
                bpm = int(float(bpm_text))
                log_message(f"BPM depuis métadonnées: {bpm}")
    except Exception as e:
        print(f"Could not read BPM from metadata: {e}")
    
    if bpm is None:
        log_message(f"⚠️ Pas de BPM dans les métadonnées originales")
    
    # FORCE MAIN ONLY MODE FOR ALL GENRES (TEMPORARY OVERRIDE)
    # Check genre to determine if we should generate full edits or just preserve original
    try:
        original_audio = MP3(original_path, ID3=ID3)
        original_tags = original_audio.tags
        genre = str(original_tags.get('TCON', '')).lower() if original_tags and 'TCON' in original_tags else ''
    except:
        original_tags = None
        genre = ''
    
    # Get original title from metadata (fallback to filename if not available)
    original_title = None
    if original_tags and 'TIT2' in original_tags:
        original_title = str(original_tags['TIT2'].text[0]) if original_tags['TIT2'].text else None
    
    # Determine the base name for output files and folders (from metadata title)
    fallback_name, _ = clean_filename(base_filename)
    if original_title:
        # Clean the metadata title for use in filename (remove invalid chars)
        metadata_base_name = original_title
        metadata_base_name = re.sub(r'[<>:"/\\|?*]', '', metadata_base_name)
        metadata_base_name = metadata_base_name.strip()
    else:
        metadata_base_name = fallback_name
    
    # Create correct output directory using metadata title
    correct_output_path = os.path.join(PROCESSED_FOLDER, metadata_base_name)
    os.makedirs(correct_output_path, exist_ok=True)
    
    # Genres that should NOT get edits (just original MP3/WAV)
    # simple_genres = ['house', 'electro house', 'dance']
    
    edits = []

    def export_edit(audio_segment, suffix):
        from concurrent.futures import ThreadPoolExecutor
        
        # Use metadata_base_name computed above
        base_name = metadata_base_name
        
        out_name_mp3 = f"{base_name} - {suffix}.mp3"
        out_name_wav = f"{base_name} - {suffix}.wav"
        
        # Use correct_output_path (based on metadata title)
        out_path_mp3 = os.path.join(correct_output_path, out_name_mp3)
        out_path_wav = os.path.join(correct_output_path, out_name_wav)
        
        # Metadata title uses the same base name + suffix
        metadata_title = f"{base_name} - {suffix}"
        
        # Parallel export of MP3 and WAV for speed
        def export_mp3():
            audio_segment.export(out_path_mp3, format="mp3", bitrate="320k")
            update_metadata(out_path_mp3, "ID By Rivoli", metadata_title, original_path, bpm)
        
        def export_wav():
            audio_segment.export(out_path_wav, format="wav")
            update_metadata_wav(out_path_wav, "ID By Rivoli", metadata_title, original_path, bpm)
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Export both formats in parallel
            futures = [executor.submit(export_mp3), executor.submit(export_wav)]
            for f in futures:
                f.result()  # Wait for completion
        
        # Use base_name (from metadata) for subdirectory and URLs
        subdir = base_name
        
        # New robust URL format using query parameter
        # Path relative to PROCESSED_FOLDER: "Subdir/Filename.mp3"
        rel_path_mp3 = f"{subdir}/{out_name_mp3}"
        rel_path_wav = f"{subdir}/{out_name_wav}"
        
        # IMPORTANT: safe='/' to NOT encode the slash!
        mp3_url = f"/download_file?path={urllib.parse.quote(rel_path_mp3, safe='/')}"
        wav_url = f"/download_file?path={urllib.parse.quote(rel_path_wav, safe='/')}"
        
        # VERIFICATION: Check if files actually exist where we expect them
        expected_mp3_path = os.path.join(PROCESSED_FOLDER, rel_path_mp3)
        expected_wav_path = os.path.join(PROCESSED_FOLDER, rel_path_wav)
        
        print(f"\n{'='*60}")
        print(f"📁 FILE GENERATION CHECK:")
        print(f"   Subdir (base_name): '{subdir}'")
        print(f"   MP3 filename: '{out_name_mp3}'")
        print(f"   WAV filename: '{out_name_wav}'")
        print(f"   ")
        print(f"   Expected MP3 path: {expected_mp3_path}")
        print(f"   MP3 EXISTS: {os.path.exists(expected_mp3_path)}")
        print(f"   ")
        print(f"   Expected WAV path: {expected_wav_path}")
        print(f"   WAV EXISTS: {os.path.exists(expected_wav_path)}")
        print(f"   ")
        # Get the full URL with base
        base_url = CURRENT_HOST_URL if CURRENT_HOST_URL else "http://localhost:8888"
        full_mp3_url = f"{base_url}{mp3_url}"
        full_wav_url = f"{base_url}{wav_url}"
        
        print(f"   Generated MP3 URL: {full_mp3_url}")
        print(f"   Generated WAV URL: {full_wav_url}")
        print(f"{'='*60}\n")
        
        # Log to UI as well - FULL URLs
        log_message(f"📥 URL MP3: {full_mp3_url}")
        log_message(f"📥 URL WAV: {full_wav_url}")
        
        # Prepare and send track info to API (for MP3)
        track_info_mp3 = {
            'type': suffix,
            'format': 'MP3',
            'name': f"{base_name} - {suffix}",
            'url': mp3_url
        }
        track_data_mp3 = prepare_track_metadata(track_info_mp3, original_path, bpm)
        if track_data_mp3:
            send_track_info_to_api(track_data_mp3)
        
        # Prepare and send track info to API (for WAV)
        track_info_wav = {
            'type': suffix,
            'format': 'WAV',
            'name': f"{base_name} - {suffix}",
            'url': wav_url
        }
        track_data_wav = prepare_track_metadata(track_info_wav, original_path, bpm)
        if track_data_wav:
            send_track_info_to_api(track_data_wav)
        
        return {
            'name': f"{base_name} - {suffix}",
            'mp3': mp3_url,
            'wav': wav_url
        }
    
    # Detect if track contains vocals by analyzing the vocals file
    def has_vocals(vocals_file_path, threshold_db=-35):
        """
        Analyzes vocals track to detect if it contains actual vocals.
        Returns True if vocals detected, False if mostly silence (instrumental track).
        """
        try:
            vocals_audio = AudioSegment.from_mp3(vocals_file_path)
            # Calculate RMS (Root Mean Square) level in dBFS
            rms_db = vocals_audio.dBFS
            # Calculate peak level
            peak_db = vocals_audio.max_dBFS
            
            print(f"   🎤 Analyse vocale: RMS={rms_db:.1f}dB, Peak={peak_db:.1f}dB (seuil={threshold_db}dB)")
            
            # If RMS is below threshold, consider it as no vocals (instrumental)
            if rms_db < threshold_db:
                return False
            return True
        except Exception as e:
            print(f"   ⚠️ Erreur analyse vocale: {e}")
            return True  # Default to True (export acapella) if analysis fails
    
    # Check if vocals exist
    vocals_detected = False
    if vocals_path and os.path.exists(vocals_path):
        vocals_detected = has_vocals(vocals_path)
        if vocals_detected:
            log_message(f"🎤 Voix détectées → Export Main + Acapella + Instrumental")
        else:
            log_message(f"🎵 Instrumental détecté (pas de voix) → Export Main + Instrumental uniquement")
    
    # Export versions based on detection
    log_message(f"Génération des versions pour : {base_filename}")
    
    # 1. Main (Original) - Always
    original = AudioSegment.from_mp3(original_path)
    edits.append(export_edit(original, "Main"))
    
    # 2. Acapella (Vocals only) - Only if vocals detected
    if vocals_path and os.path.exists(vocals_path) and vocals_detected:
        vocals = AudioSegment.from_mp3(vocals_path)
        edits.append(export_edit(vocals, "Acapella"))
        log_message(f"✓ Version Acapella créée")
    elif vocals_path and os.path.exists(vocals_path) and not vocals_detected:
        log_message(f"⏭️ Acapella ignorée (pas de voix détectées)")
    else:
        log_message(f"⚠️ Pas de fichier vocals pour Acapella")
    
    # 3. Instrumental (No vocals) - Always if available
    if inst_path and os.path.exists(inst_path):
        instrumental = AudioSegment.from_mp3(inst_path)
        edits.append(export_edit(instrumental, "Instrumental"))
        log_message(f"✓ Version Instrumentale créée")
    else:
        log_message(f"⚠️ Pas de fichier instrumental")
    
    # Register track as pending download - files stay until all are downloaded
    # Count actual files: each edit has MP3 + WAV = 2 files per edit
    num_files = len(edits) * 2
    
    # Build list of all actual file names for sequential download tracking
    # Each edit has format: {"name": "Track - Main", "mp3": "/download_file?path=...", "wav": "/download_file?path=..."}
    file_list = []
    for edit in edits:
        # Extract actual file names from the edit
        # The format is "{base_name} - {suffix}.mp3" and "{base_name} - {suffix}.wav"
        edit_name = edit['name']
        file_list.append(f"{edit_name}.mp3")
        file_list.append(f"{edit_name}.wav")
    
    track_file_for_pending_download(metadata_base_name, original_path, num_files, file_list)

    return edits

def run_demucs_thread(filepaths, original_filenames):
    global job_status
    try:
        job_status['state'] = 'processing'
        job_status['total_files'] = len(filepaths)
        job_status['results'] = []
        job_status['progress'] = 0

        current_file_index = 0

        for i in range(0, len(filepaths), 50):
            chunk = filepaths[i:i + 50]
            
            # Optimized Demucs settings for batch processing (H100 + 240GB RAM)
            if DEMUCS_DEVICE == 'cuda':
                try:
                    import torch
                    import psutil
                    gpu_mem_gb = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                    ram_gb = psutil.virtual_memory().total / (1024**3)
                    
                    if gpu_mem_gb >= 70 and ram_gb >= 200:
                        batch_jobs = 20  # Maximum for H100 + high RAM
                    elif gpu_mem_gb >= 70:
                        batch_jobs = 16
                    elif gpu_mem_gb >= 40:
                        batch_jobs = 12
                    else:
                        batch_jobs = 8
                except:
                    batch_jobs = 8
            else:
                batch_jobs = max(4, CPU_COUNT)
            
            command = [
                'python3', '-m', 'demucs',
                '--two-stems=vocals',
                '-n', 'htdemucs',
                '--mp3',
                '--mp3-bitrate', '320',
                '-j', str(batch_jobs),        # Maximum parallelism
                '--segment', '7',             # Max segment size (integer)
                '--overlap', '0.1',           # Minimal for speed
                '--device', DEMUCS_DEVICE,    # GPU/CPU auto-detection
                '-o', OUTPUT_FOLDER
            ] + chunk

            chunk_num = i // 50 + 1
            total_chunks = (len(filepaths) - 1) // 50 + 1
            log_message(f"Démarrage de la séparation IA (Lot {chunk_num}/{total_chunks})...")
            
            # Reset progress for new chunk relative to file count? 
            # Ideally we track global file index.
            
            process = subprocess.Popen(
                command, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True, 
                bufsize=1, 
                universal_newlines=True
            )

            current_chunk_base = i

            for line in process.stdout:
                print(line, end='')
                
                if "Separating track" in line:
                    # Parse filename from line if possible, or just increment
                    # Demucs output: "Separating track filename.mp3"
                    try:
                        match = re.search(r"Separating track\s+(.+)$", line)
                        if match:
                            filename_found = match.group(1).strip()
                            job_status['current_filename'] = filename_found
                            log_message(f"Séparation en cours : {filename_found}")
                    except:
                        pass

                    current_file_index += 1
                    job_status['current_file_idx'] = current_file_index
                    
                    # Calculate global progress (0-50%)
                    # Phase 1 is separation (0-50%), Phase 2 is editing (50-100%)
                    # Actually, Demucs takes most of the time. Let's say Demucs is 0-90%?
                    # The user prompt implies Edit generation is fast.
                    # But previous code had 0-50 / 50-100.
                    # Let's keep 0-50 for Demucs for now, but update UI to be clearer.
                    
                    percent_per_file = 50 / len(filepaths)
                    base_progress = (current_file_index - 1) * percent_per_file
                    job_status['progress'] = int(base_progress)
                    job_status['current_step'] = f"Séparation IA (Lot {chunk_num}/{total_chunks})"

                elif "%|" in line:
                    # Demucs progress bar " 15%|███      | 20/130 [00:05<00:25,  4.23it/s]"
                    try:
                        # Extract percentage
                        parts = line.split('%|')
                        if len(parts) > 0:
                            percent_part = parts[0].strip()
                            # Use regex to find last number before %
                            p_match = re.search(r'(\d+)$', percent_part)
                            if p_match:
                                track_percent = int(p_match.group(1))
                                
                                # Add fractional progress for current file
                                percent_per_file = 50 / len(filepaths)
                                base_progress = (current_file_index - 1) * percent_per_file
                                added_progress = (track_percent / 100) * percent_per_file
                                job_status['progress'] = int(base_progress + added_progress)
                    except:
                        pass
            
            process.wait()
            
            if process.returncode != 0:
                job_status['state'] = 'error'
                job_status['error'] = 'Erreur lors du traitement Demucs'
                return

        print("Starting Edit Generation Phase...")
        log_message("Fin de la séparation IA. Début de la génération des Edits DJ...")
        job_status['progress'] = 50
        job_status['current_step'] = "Génération des Edits"
        
        all_results = []
        results_lock = threading.Lock()
        completed_count = [0]  # Use list for mutable in closure
        
        def process_single_edit(filepath):
            """Process a single track's edits - can run in parallel"""
            filename = os.path.basename(filepath)
            track_name = os.path.splitext(filename)[0]
            
            log_message(f"🔄 Création des edits pour : {filename}")
            
            source_dir = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
            inst_path = os.path.join(source_dir, 'no_vocals.mp3')
            vocals_path = os.path.join(source_dir, 'vocals.mp3')
            
            if os.path.exists(inst_path) and os.path.exists(vocals_path):
                clean_name, _ = clean_filename(filename)
                track_output_dir = os.path.join(PROCESSED_FOLDER, clean_name)
                os.makedirs(track_output_dir, exist_ok=True)
                
                edits = create_edits(vocals_path, inst_path, filepath, track_output_dir, filename)
                
                with results_lock:
                    all_results.append({
                        'original': clean_name,
                        'edits': edits
                    })
                    completed_count[0] += 1
                    job_status['progress'] = 50 + int(completed_count[0] / len(filepaths) * 50)
                    job_status['current_filename'] = f"{completed_count[0]}/{len(filepaths)} terminés"
            else:
                print(f"Warning: Output files not found for {track_name}")
        
        # Process edits in parallel using ThreadPoolExecutor
        # With 240GB RAM and 20 vCPUs, we can max out edit workers
        edit_workers = max(8, min(NUM_WORKERS, CPU_COUNT))
        print(f"🚀 Génération des edits avec {edit_workers} workers parallèles")
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=edit_workers) as executor:
            futures = [executor.submit(process_single_edit, fp) for fp in filepaths]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Edit generation error: {e}")

        job_status['progress'] = 100
        job_status['results'] = all_results
        job_status['state'] = 'completed'

    except Exception as e:
        print(f"Error in thread: {e}")
        job_status['state'] = 'error'
        job_status['error'] = str(e)

def get_git_info():
    try:
        # Get hash
        hash_output = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).strip().decode('utf-8')
        # Get date
        date_output = subprocess.check_output(['git', 'log', '-1', '--format=%cd', '--date=format:%a %b %d %H:%M']).strip().decode('utf-8')
        
        # Get count of commits to simulate version number if needed, or just use hardcoded base
        # Using a simple counter for versioning: v0.20 + (commits since last tag or simple count)
        # For now, let's keep it simple: just show the hash/date dynamically.
        # But user asked for "Version update tout seul".
        # Let's count total commits as a "build number" or similar.
        count = subprocess.check_output(['git', 'rev-list', '--count', 'HEAD']).strip().decode('utf-8')
        
        return f"v0.{count} ({hash_output}) - {date_output}"
    except:
        return "Dev Version"

@app.route('/')
def index():
    version_info = get_git_info()
    return render_template('index.html', version_info=version_info)

import queue

import json

# =============================================================================
# DUPLICATE/ALREADY PROCESSED TRACK DETECTION
# =============================================================================
def is_track_already_processed(filename):
    """
    Check if a track has already been processed.
    Returns (is_processed: bool, processed_dir: str or None)
    
    Checks:
    1. If the track folder exists in PROCESSED_FOLDER
    2. If the track is in pending_downloads (processed, waiting for download)
    """
    clean_name, _ = clean_filename(filename)
    
    # Check if track folder exists in PROCESSED_FOLDER
    track_folder = os.path.join(PROCESSED_FOLDER, clean_name)
    if os.path.exists(track_folder) and os.path.isdir(track_folder):
        # Check if it has actual files inside (not empty folder)
        files = [f for f in os.listdir(track_folder) if f.endswith(('.mp3', '.wav'))]
        if files:
            return True, track_folder
    
    # Check if track is in pending_downloads
    with pending_downloads_lock:
        if clean_name in pending_downloads:
            return True, pending_downloads[clean_name].get('processed_dir', track_folder)
    
    return False, None

def get_already_processed_tracks():
    """Get list of all track names that have already been processed."""
    processed_tracks = set()
    
    # From PROCESSED_FOLDER
    if os.path.exists(PROCESSED_FOLDER):
        for item in os.listdir(PROCESSED_FOLDER):
            item_path = os.path.join(PROCESSED_FOLDER, item)
            if os.path.isdir(item_path):
                # Check if it has actual files
                files = [f for f in os.listdir(item_path) if f.endswith(('.mp3', '.wav'))]
                if files:
                    processed_tracks.add(item)
    
    # From pending_downloads
    with pending_downloads_lock:
        processed_tracks.update(pending_downloads.keys())
    
    return list(processed_tracks)

# Auto-detect optimal number of workers based on CPU/GPU
import multiprocessing
CPU_COUNT = multiprocessing.cpu_count()

# =============================================================================
# OPTIMIZED FOR: H100 80GB VRAM | 20 vCPU | 240GB RAM | 5TB Scratch
# =============================================================================
def get_optimal_workers():
    """Calculate optimal workers based on available resources."""
    import psutil
    
    # Get system RAM
    ram_gb = psutil.virtual_memory().total / (1024**3)
    print(f"💾 System RAM: {ram_gb:.0f}GB")
    
    try:
        import torch
        if torch.cuda.is_available():
            gpu_mem_gb = torch.cuda.get_device_properties(0).total_memory / (1024**3)
            gpu_name = torch.cuda.get_device_name(0)
            
            # H100 80GB + 240GB RAM: Maximum throughput
            # Each Demucs process uses ~6-8GB VRAM + ~4GB RAM
            # With 80GB VRAM we can run ~10 concurrent GPU processes
            # With 240GB RAM we're not RAM-limited
            if gpu_mem_gb >= 70 and ram_gb >= 200:  # H100 + High RAM
                num_workers = 16  # Maximum throughput
                print(f"🚀 H100 80GB + {ram_gb:.0f}GB RAM: {num_workers} parallel workers (MAXIMUM)")
            elif gpu_mem_gb >= 70:  # H100 80GB
                num_workers = 12
                print(f"🚀 H100 80GB: {num_workers} parallel workers")
            elif gpu_mem_gb >= 40:  # A100
                num_workers = 8
                print(f"🚀 A100 ({gpu_mem_gb:.0f}GB): {num_workers} parallel workers")
            elif gpu_mem_gb >= 20:  # RTX 3090/4090
                num_workers = 4
                print(f"🚀 GPU ({gpu_mem_gb:.0f}GB): {num_workers} parallel workers")
            else:
                num_workers = 2
                print(f"🚀 GPU ({gpu_mem_gb:.0f}GB): {num_workers} parallel workers")
            
            return num_workers
    except Exception as e:
        print(f"⚠️ GPU detection error: {e}")
    
    # CPU fallback: with 240GB RAM, we can use more CPU workers
    if ram_gb >= 200:
        return min(16, CPU_COUNT)
    return max(2, min(8, CPU_COUNT // 2))

NUM_WORKERS = get_optimal_workers()
print(f"🔧 Configuration: {CPU_COUNT} CPUs détectés → {NUM_WORKERS} workers parallèles")

# =============================================================================
# BATCH TRACKING: Track processed count (NO PAUSE, NO AUTO-DELETE)
# =============================================================================
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 200))  # Log milestone every N tracks
BATCH_MODE_ENABLED = os.environ.get('BATCH_MODE', 'true').lower() == 'true'  # Enable/disable batch tracking

batch_processed_count = 0
batch_lock = Lock()

def increment_batch_count():
    """Increment the batch counter for tracking purposes (no pause, no delete)."""
    global batch_processed_count
    
    if not BATCH_MODE_ENABLED:
        return
    
    with batch_lock:
        batch_processed_count += 1
        count = batch_processed_count
        
        # Log progress every BATCH_SIZE tracks
        if count % BATCH_SIZE == 0:
            print(f"📊 Milestone: {count} tracks processed (continuous processing, no pause)")
            log_message(f"📊 {count} tracks traités (traitement continu)")

def wait_for_batch_resume():
    """Legacy function - no longer pauses, kept for compatibility."""
    pass  # No pause - continuous processing

print(f"📦 Batch tracking: {'ENABLED' if BATCH_MODE_ENABLED else 'DISABLED'} (milestone every {BATCH_SIZE} tracks, no pause)")

# =============================================================================
# DISK-BASED CLEANUP: Delete oldest 25k tracks when disk reaches 80%
# =============================================================================
DISK_THRESHOLD_PERCENT = int(os.environ.get('DISK_THRESHOLD_PERCENT', 80))  # Trigger cleanup at 80% disk usage
TRACKS_TO_DELETE = int(os.environ.get('TRACKS_TO_DELETE', 25000))  # Delete 25k oldest tracks when triggered
DISK_CHECK_INTERVAL_SECONDS = int(os.environ.get('DISK_CHECK_INTERVAL', 60))  # Check disk every 60 seconds
DISK_CLEANUP_ENABLED = os.environ.get('DISK_CLEANUP', 'true').lower() == 'true'

disk_cleanup_in_progress = False
disk_cleanup_lock = Lock()

def get_disk_usage_percent():
    """Get current disk usage percentage."""
    try:
        disk = psutil.disk_usage('/')
        return disk.percent
    except Exception as e:
        print(f"⚠️ Could not get disk usage: {e}")
        return 0

def get_oldest_tracks(limit):
    """
    Get list of oldest track folders in PROCESSED_FOLDER sorted by modification time.
    Returns list of tuples: (track_name, full_path, mtime)
    """
    tracks = []
    
    try:
        if not os.path.exists(PROCESSED_FOLDER):
            return []
        
        for item in os.listdir(PROCESSED_FOLDER):
            item_path = os.path.join(PROCESSED_FOLDER, item)
            if os.path.isdir(item_path):
                try:
                    mtime = os.path.getmtime(item_path)
                    tracks.append((item, item_path, mtime))
                except Exception as e:
                    print(f"   ⚠️ Could not get mtime for {item}: {e}")
        
        # Sort by modification time (oldest first)
        tracks.sort(key=lambda x: x[2])
        
        return tracks[:limit]
    
    except Exception as e:
        print(f"⚠️ Error getting oldest tracks: {e}")
        return []

def delete_oldest_tracks(count):
    """
    Delete the oldest N track folders from PROCESSED_FOLDER.
    Also cleans up associated htdemucs folders and tracking data.
    Returns number of tracks actually deleted.
    """
    global disk_cleanup_in_progress
    
    with disk_cleanup_lock:
        if disk_cleanup_in_progress:
            print("⚠️ Disk cleanup already in progress, skipping...")
            return 0
        disk_cleanup_in_progress = True
    
    deleted_count = 0
    freed_bytes = 0
    
    try:
        oldest_tracks = get_oldest_tracks(count)
        
        if not oldest_tracks:
            print("ℹ️ No tracks found to delete")
            return 0
        
        print(f"")
        print(f"🗑️ ════════════════════════════════════════════════")
        print(f"🗑️ DISK CLEANUP: Deleting {len(oldest_tracks)} oldest tracks...")
        print(f"🗑️ ════════════════════════════════════════════════")
        
        for track_name, track_path, mtime in oldest_tracks:
            try:
                # Calculate size before deletion
                track_size = 0
                for dirpath, dirnames, filenames in os.walk(track_path):
                    for f in filenames:
                        fp = os.path.join(dirpath, f)
                        try:
                            track_size += os.path.getsize(fp)
                        except:
                            pass
                
                # Delete the track folder
                shutil.rmtree(track_path)
                deleted_count += 1
                freed_bytes += track_size
                
                # Also delete htdemucs intermediate folder
                htdemucs_folder = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
                if os.path.exists(htdemucs_folder):
                    for dirpath, dirnames, filenames in os.walk(htdemucs_folder):
                        for f in filenames:
                            fp = os.path.join(dirpath, f)
                            try:
                                freed_bytes += os.path.getsize(fp)
                            except:
                                pass
                    shutil.rmtree(htdemucs_folder)
                
                # Clean up tracking data
                with pending_downloads_lock:
                    if track_name in pending_downloads:
                        del pending_downloads[track_name]
                
                with track_download_status_lock:
                    if track_name in track_download_status:
                        del track_download_status[track_name]
                
                with scheduled_deletions_lock:
                    if track_name in scheduled_deletions:
                        del scheduled_deletions[track_name]
                
                if deleted_count % 1000 == 0:
                    print(f"   🗑️ Progress: {deleted_count}/{len(oldest_tracks)} tracks deleted...")
                
            except Exception as e:
                print(f"   ⚠️ Error deleting {track_name}: {e}")
        
        freed_mb = freed_bytes / (1024 * 1024)
        freed_gb = freed_bytes / (1024 * 1024 * 1024)
        
        print(f"")
        print(f"✅ ════════════════════════════════════════════════")
        print(f"✅ DISK CLEANUP COMPLETE")
        print(f"✅ Deleted: {deleted_count} tracks")
        if freed_gb >= 1:
            print(f"✅ Freed: {freed_gb:.2f} GB")
        else:
            print(f"✅ Freed: {freed_mb:.1f} MB")
        print(f"✅ New disk usage: {get_disk_usage_percent():.1f}%")
        print(f"✅ ════════════════════════════════════════════════")
        
        log_message(f"🗑️ Disk cleanup: {deleted_count} oldest tracks deleted, {freed_gb:.2f} GB freed")
        
    finally:
        with disk_cleanup_lock:
            disk_cleanup_in_progress = False
    
    return deleted_count

def disk_monitor_loop():
    """Background thread that monitors disk usage and triggers cleanup when needed."""
    while True:
        try:
            usage = get_disk_usage_percent()
            
            if usage >= DISK_THRESHOLD_PERCENT:
                print(f"")
                print(f"⚠️ ════════════════════════════════════════════════")
                print(f"⚠️ DISK USAGE ALERT: {usage:.1f}% (threshold: {DISK_THRESHOLD_PERCENT}%)")
                print(f"⚠️ Starting cleanup of {TRACKS_TO_DELETE} oldest tracks...")
                print(f"⚠️ ════════════════════════════════════════════════")
                
                log_message(f"⚠️ Disk usage {usage:.1f}% >= {DISK_THRESHOLD_PERCENT}% - Starting cleanup")
                
                deleted = delete_oldest_tracks(TRACKS_TO_DELETE)
                
                if deleted > 0:
                    new_usage = get_disk_usage_percent()
                    log_message(f"✅ Cleanup complete: {deleted} tracks deleted, disk now at {new_usage:.1f}%")
            
        except Exception as e:
            print(f"⚠️ Disk monitor error: {e}")
        
        time.sleep(DISK_CHECK_INTERVAL_SECONDS)

# Start disk monitor thread
if DISK_CLEANUP_ENABLED:
    disk_monitor_thread = threading.Thread(target=disk_monitor_loop, daemon=True)
    disk_monitor_thread.start()
    print(f"💾 Disk cleanup: ENABLED (threshold={DISK_THRESHOLD_PERCENT}%, delete={TRACKS_TO_DELETE} oldest tracks)")
else:
    print(f"💾 Disk cleanup: DISABLED")

# =============================================================================
# DELAYED DELETION: Delete downloaded files after X minutes
# =============================================================================
DELAYED_DELETE_MINUTES = int(os.environ.get('DELAYED_DELETE_MINUTES', 5))  # Delete files 5 min after download
DELAYED_DELETE_ENABLED = os.environ.get('DELAYED_DELETE', 'true').lower() == 'true'

# Track scheduled deletions to avoid duplicates
scheduled_deletions = {}
scheduled_deletions_lock = Lock()

def schedule_track_deletion(track_name, delay_minutes=None):
    """Schedule a track folder for deletion after delay_minutes."""
    if not DELAYED_DELETE_ENABLED:
        return
    
    if delay_minutes is None:
        delay_minutes = DELAYED_DELETE_MINUTES
    
    with scheduled_deletions_lock:
        if track_name in scheduled_deletions:
            print(f"   ⏰ Deletion already scheduled for '{track_name}'")
            return
        scheduled_deletions[track_name] = time.time()
    
    print(f"   ⏰ Scheduling deletion of '{track_name}' in {delay_minutes} minutes")
    
    # Start deletion timer thread
    timer_thread = threading.Thread(
        target=delayed_delete_track,
        args=(track_name, delay_minutes),
        daemon=True
    )
    timer_thread.start()

def delayed_delete_track(track_name, delay_minutes):
    """Wait for delay then delete the track folder."""
    delay_seconds = delay_minutes * 60
    
    print(f"⏰ [{track_name}] Will be deleted in {delay_minutes} min ({delay_seconds}s)")
    time.sleep(delay_seconds)
    
    print(f"")
    print(f"🗑️ ════════════════════════════════════════════════")
    print(f"🗑️ DELAYED DELETE: '{track_name}' (after {delay_minutes}min)")
    print(f"🗑️ ════════════════════════════════════════════════")
    
    try:
        # Delete the track folder from processed
        track_folder = os.path.join(PROCESSED_FOLDER, track_name)
        if os.path.exists(track_folder):
            shutil.rmtree(track_folder)
            print(f"   🗑️ Deleted processed folder: {track_folder}")
        else:
            print(f"   ℹ️ Processed folder already deleted: {track_folder}")
        
        # Delete htdemucs intermediate files
        htdemucs_folder = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
        if os.path.exists(htdemucs_folder):
            shutil.rmtree(htdemucs_folder)
            print(f"   🗑️ Deleted htdemucs folder: {htdemucs_folder}")
        
        # Clean up from tracking systems
        try:
            cleanup_track_after_downloads(track_name)
        except:
            pass
        
        try:
            with pending_downloads_lock:
                if track_name in pending_downloads:
                    del pending_downloads[track_name]
        except:
            pass
        
        log_message(f"🗑️ Deleted '{track_name}' after {delay_minutes}min delay")
        
    except Exception as e:
        print(f"   ⚠️ Error deleting '{track_name}': {e}")
    
    finally:
        # Remove from scheduled deletions
        with scheduled_deletions_lock:
            if track_name in scheduled_deletions:
                del scheduled_deletions[track_name]

print(f"⏰ Delayed delete: {'ENABLED' if DELAYED_DELETE_ENABLED else 'DISABLED'} ({DELAYED_DELETE_MINUTES}min after download)")

# Global Queue for processing tracks
track_queue = queue.Queue()

# Track queue items with status for UI display
# Structure: { "filename": { "status": "waiting|processing|done", "worker": None|worker_id, "progress": 0-100 } }
queue_items = {}
queue_items_lock = Lock()

# Maximum time an item can stay in 'processing' state before being marked as stale (in seconds)
MAX_PROCESSING_TIME = 30 * 60  # 30 minutes

def add_to_queue_tracker(filename, session_id):
    """Add item to queue tracker for UI display."""
    with queue_items_lock:
        queue_items[filename] = {
            'status': 'waiting',
            'worker': None,
            'progress': 0,
            'session_id': session_id,
            'step': 'En attente...',
            'added_at': time.time(),
            'processing_started_at': None
        }

def update_queue_item(filename, status=None, worker=None, progress=None, step=None):
    """Update queue item status."""
    with queue_items_lock:
        if filename in queue_items:
            if status:
                queue_items[filename]['status'] = status
                # Track when processing started
                if status == 'processing':
                    queue_items[filename]['processing_started_at'] = time.time()
            if worker is not None: queue_items[filename]['worker'] = worker
            if progress is not None: queue_items[filename]['progress'] = progress
            if step: queue_items[filename]['step'] = step

def remove_from_queue_tracker(filename):
    """Remove item from queue tracker."""
    with queue_items_lock:
        if filename in queue_items:
            del queue_items[filename]

def cleanup_stale_processing_items():
    """
    Clean up items that have been stuck in 'processing' state for too long.
    This handles cases where a worker crashed without proper cleanup.
    """
    current_time = time.time()
    stale_items = []
    
    with queue_items_lock:
        for filename, info in queue_items.items():
            if info['status'] == 'processing':
                started_at = info.get('processing_started_at')
                if started_at and (current_time - started_at) > MAX_PROCESSING_TIME:
                    stale_items.append(filename)
    
    # Mark stale items as failed (outside lock to avoid deadlock)
    for filename in stale_items:
        print(f"⚠️ Cleaning up stale processing item: {filename}")
        with queue_items_lock:
            if filename in queue_items:
                session_id = queue_items[filename].get('session_id', 'global')
                queue_items[filename]['status'] = 'failed'
                queue_items[filename]['step'] = '❌ Timeout: traitement trop long'
                queue_items[filename]['worker'] = None
        
        # Add to failed files
        try:
            add_failed_file(session_id, filename, "Timeout: le traitement a pris trop de temps", None)
        except:
            pass
    
    return len(stale_items)

def get_queue_items_list():
    """Get list of queue items for UI."""
    # First, cleanup any stale items
    cleanup_stale_processing_items()
    
    with queue_items_lock:
        items = []
        processing_count = 0
        
        for filename, info in queue_items.items():
            item_data = {
                'filename': filename,
                'status': info['status'],
                'worker': info['worker'],
                'progress': info['progress'],
                'step': info['step']
            }
            
            # Safety check: count processing items
            if info['status'] == 'processing':
                processing_count += 1
                # If we have more processing items than workers, something is wrong
                # Mark excess items as waiting (should not happen with the fixes)
                if processing_count > NUM_WORKERS:
                    print(f"⚠️ Too many processing items detected! Resetting {filename} to waiting")
                    info['status'] = 'waiting'
                    info['worker'] = None
                    info['processing_started_at'] = None
                    item_data['status'] = 'waiting'
                    item_data['worker'] = None
            
            items.append(item_data)
        
        # Sort: failed first (for visibility), then processing, then waiting
        status_order = {'failed': 0, 'processing': 1, 'waiting': 2}
        items.sort(key=lambda x: (status_order.get(x['status'], 3), x['filename']))
        return items

# Worker thread function
def worker(worker_id):
    while True:
        current_filename = None  # Track current file for cleanup on exception
        current_session_id = 'global'
        try:
            # Wait if batch is paused
            wait_for_batch_resume()
            
            queue_item = track_queue.get()
            if queue_item is None:
                break
            
            # Handle both old format (string) and new format (dict with session_id)
            if isinstance(queue_item, dict):
                filename = queue_item['filename']
                session_id = queue_item.get('session_id', 'global')
                is_retry = queue_item.get('is_retry', False)
            else:
                filename = queue_item
                session_id = 'global'
                is_retry = False
            
            # Store for exception handler cleanup
            current_filename = filename
            current_session_id = session_id
            
            # Get session-specific status
            current_status = get_job_status(session_id)
            
            # Build filepath with session-specific folder
            session_upload_folder = os.path.join(UPLOAD_FOLDER, session_id)
            filepath = os.path.join(session_upload_folder, filename)
            
            # Fallback to global folder if not found in session folder
            if not os.path.exists(filepath):
                filepath = os.path.join(UPLOAD_FOLDER, filename)
            
            # Check if file exists with retries (handles race condition with upload)
            file_found = os.path.exists(filepath)
            if not file_found:
                # Retry up to 5 times with 1 second delay (file might still be uploading)
                for retry in range(5):
                    time.sleep(1)
                    # Re-check both locations
                    filepath = os.path.join(session_upload_folder, filename)
                    if os.path.exists(filepath):
                        file_found = True
                        print(f"   ✅ File found after {retry + 1} retry(ies): {filename}")
                        break
                    filepath = os.path.join(UPLOAD_FOLDER, filename)
                    if os.path.exists(filepath):
                        file_found = True
                        print(f"   ✅ File found in global folder after {retry + 1} retry(ies): {filename}")
                        break
                    print(f"   ⏳ Waiting for file ({retry + 1}/5): {filename}")
            
            if not file_found:
                error_msg = f"Fichier introuvable après 5 tentatives: {filename}"
                log_message(f"⚠️ {error_msg}", session_id)
                log_message(f"   Chemins vérifiés: {session_upload_folder}/{filename} et {UPLOAD_FOLDER}/{filename}", session_id)
                
                # Track as failed file
                add_failed_file(session_id, filename, error_msg, filepath)
                update_queue_item(filename, status='failed', progress=0, step='❌ Fichier introuvable')
                
                # Don't remove from tracker - keep showing as failed
                track_queue.task_done()
                current_filename = None  # Clear so exception handler doesn't double-process
                if track_queue.empty():
                    current_status['state'] = 'idle'
                    current_status['current_step'] = ''
                    current_status['current_filename'] = ''
                continue

            # Update tracker: now processing
            update_queue_item(filename, status='processing', worker=worker_id, progress=0, step='Démarrage...')
            
            print(f"🔄 Worker {worker_id} traite: {filename}" + (" (RETRY)" if is_retry else ""))
            success, error_msg = process_single_track(filepath, filename, session_id, worker_id, is_retry)
            
            # Handle result
            if success:
                # Remove from tracker when done successfully
                remove_from_queue_tracker(filename)
                
                # Increment batch counter (may trigger pause after BATCH_SIZE tracks)
                increment_batch_count()
            else:
                # Update queue item to show failed status (process_single_track should have done this, but ensure it)
                update_queue_item(filename, status='failed', progress=0, step=f'❌ {error_msg[:50] if error_msg else "Erreur inconnue"}...')
                log_message(f"❌ [{session_id}] Worker {worker_id}: Échec pour {filename}: {error_msg}", session_id)
            
            track_queue.task_done()
            current_filename = None  # Clear so exception handler doesn't double-process
            
            # Reset state to idle if queue is empty
            if track_queue.empty():
                current_status['state'] = 'idle'
                failed_count = len(current_status.get('failed_files', []))
                if failed_count > 0:
                    current_status['current_step'] = f'⚠️ {failed_count} fichier(s) en échec - Cliquer "Réessayer" pour relancer'
                else:
                    current_status['current_step'] = 'Prêt pour de nouveaux fichiers'
                current_status['current_filename'] = ''
                log_message(f"✅ File d'attente terminée" + (f" - {failed_count} échec(s)" if failed_count > 0 else " - Tous les fichiers traités avec succès"), session_id)
                
        except Exception as e:
            print(f"Worker {worker_id} Error: {e}")
            import traceback
            traceback.print_exc()
            log_message(f"Erreur Worker {worker_id}: {e}")
            
            # CRITICAL: Clean up queue_item on exception to prevent "stuck in processing" state
            if current_filename:
                try:
                    error_msg = f"Erreur worker: {str(e)[:100]}"
                    add_failed_file(current_session_id, current_filename, error_msg, None)
                    update_queue_item(current_filename, status='failed', progress=0, step=f'❌ Crash: {str(e)[:50]}...')
                    log_message(f"🔧 [{current_session_id}] Cleaned up crashed item: {current_filename}", current_session_id)
                except Exception as cleanup_error:
                    print(f"Worker {worker_id} cleanup error: {cleanup_error}")
                
            # Try to mark task as done to prevent queue deadlock
            try:
                track_queue.task_done()
            except ValueError:
                pass  # task_done called more times than tasks

# Start multiple worker threads
worker_threads = []
for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(i+1,), daemon=True)
    t.start()
    worker_threads.append(t)
print(f"🚀 {NUM_WORKERS} workers démarrés")

# Configuration for startup cleanup
CLEANUP_ON_START = os.environ.get('CLEANUP_ON_START', 'true').lower() == 'true'
DELETE_AFTER_DOWNLOAD = os.environ.get('DELETE_AFTER_DOWNLOAD', 'false').lower() == 'true'  # Disabled by default - files deleted via /confirm_download or periodic cleanup

def startup_cleanup():
    """
    Clears all storage on server startup to ensure clean state.
    This prevents disk from filling up between restarts.
    Can be disabled by setting CLEANUP_ON_START=false
    """
    if not CLEANUP_ON_START:
        log_message("⏭️ Startup cleanup disabled (CLEANUP_ON_START=false)")
        return
    
    log_message("🧹 ════════════════════════════════════════════════")
    log_message("🧹 STARTUP CLEANUP: Clearing all storage...")
    
    total_deleted = 0
    total_size_freed = 0
    
    folders_to_clean = [
        (UPLOAD_FOLDER, "uploads"),
        (OUTPUT_FOLDER, "output"),
        (PROCESSED_FOLDER, "processed")
    ]
    
    for folder, name in folders_to_clean:
        if not os.path.exists(folder):
            continue
            
        folder_size = 0
        file_count = 0
        
        for item in os.listdir(folder):
            item_path = os.path.join(folder, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    folder_size += os.path.getsize(item_path)
                    os.unlink(item_path)
                    file_count += 1
                elif os.path.isdir(item_path):
                    # Calculate dir size first
                    for dirpath, dirnames, filenames in os.walk(item_path):
                        for f in filenames:
                            fp = os.path.join(dirpath, f)
                            try:
                                folder_size += os.path.getsize(fp)
                            except:
                                pass
                    shutil.rmtree(item_path)
                    file_count += 1
            except Exception as e:
                print(f"   ⚠️ Could not delete {item_path}: {e}")
        
        total_deleted += file_count
        total_size_freed += folder_size
        
        size_mb = folder_size / (1024 * 1024)
        if file_count > 0:
            log_message(f"   🗑️ {name}: {file_count} items deleted ({size_mb:.1f} MB)")
    
    # Also clean covers folder
    covers_folder = os.path.join(BASE_DIR, 'static', 'covers')
    if os.path.exists(covers_folder):
        cover_count = 0
        for filename in os.listdir(covers_folder):
            if filename.startswith('cover_'):  # Only delete extracted covers
                try:
                    file_path = os.path.join(covers_folder, filename)
                    total_size_freed += os.path.getsize(file_path)
                    os.unlink(file_path)
                    cover_count += 1
                except:
                    pass
        if cover_count > 0:
            log_message(f"   🗑️ covers: {cover_count} items deleted")
    
    # Clear pending downloads tracker
    with pending_downloads_lock:
        pending_downloads.clear()
    
    total_mb = total_size_freed / (1024 * 1024)
    total_gb = total_size_freed / (1024 * 1024 * 1024)
    
    if total_gb >= 1:
        log_message(f"🧹 CLEANUP COMPLETE: {total_deleted} items, {total_gb:.2f} GB freed")
    else:
        log_message(f"🧹 CLEANUP COMPLETE: {total_deleted} items, {total_mb:.1f} MB freed")
    log_message("🧹 ════════════════════════════════════════════════")

# Call startup cleanup instead of restore_queue
startup_cleanup()

# Configuration for periodic cleanup
MAX_FILE_AGE_HOURS = int(os.environ.get('MAX_FILE_AGE_HOURS', 10))  # Delete files older than 10 hours by default
CLEANUP_INTERVAL_MINUTES = int(os.environ.get('CLEANUP_INTERVAL_MINUTES', 500))  # Check every 15 minutes

def periodic_cleanup():
    """
    Background thread that periodically cleans up old files.
    Deletes processed files older than MAX_FILE_AGE_HOURS.
    """
    while True:
        try:
            time.sleep(CLEANUP_INTERVAL_MINUTES * 60)  # Sleep first, then cleanup
            
            now = time.time()
            max_age_seconds = MAX_FILE_AGE_HOURS * 3600
            
            cleaned_count = 0
            cleaned_size = 0
            
            # Clean old files in processed folder
            if os.path.exists(PROCESSED_FOLDER):
                for item in os.listdir(PROCESSED_FOLDER):
                    item_path = os.path.join(PROCESSED_FOLDER, item)
                    try:
                        # Get modification time of folder
                        mtime = os.path.getmtime(item_path)
                        age = now - mtime
                        
                        if age > max_age_seconds:
                            if os.path.isdir(item_path):
                                # Calculate size before deleting
                                for dirpath, dirnames, filenames in os.walk(item_path):
                                    for f in filenames:
                                        try:
                                            cleaned_size += os.path.getsize(os.path.join(dirpath, f))
                                        except:
                                            pass
                                shutil.rmtree(item_path)
                            else:
                                cleaned_size += os.path.getsize(item_path)
                                os.unlink(item_path)
                            cleaned_count += 1
                    except Exception as e:
                        pass
            
            # Clean old htdemucs output
            htdemucs_folder = os.path.join(OUTPUT_FOLDER, 'htdemucs')
            if os.path.exists(htdemucs_folder):
                for item in os.listdir(htdemucs_folder):
                    item_path = os.path.join(htdemucs_folder, item)
                    try:
                        mtime = os.path.getmtime(item_path)
                        age = now - mtime
                        
                        if age > max_age_seconds:
                            if os.path.isdir(item_path):
                                for dirpath, dirnames, filenames in os.walk(item_path):
                                    for f in filenames:
                                        try:
                                            cleaned_size += os.path.getsize(os.path.join(dirpath, f))
                                        except:
                                            pass
                                shutil.rmtree(item_path)
                            else:
                                cleaned_size += os.path.getsize(item_path)
                                os.unlink(item_path)
                            cleaned_count += 1
                    except:
                        pass
            
            # Clean old upload files
            if os.path.exists(UPLOAD_FOLDER):
                for item in os.listdir(UPLOAD_FOLDER):
                    item_path = os.path.join(UPLOAD_FOLDER, item)
                    try:
                        mtime = os.path.getmtime(item_path)
                        age = now - mtime
                        
                        if age > max_age_seconds:
                            if os.path.isdir(item_path):
                                for dirpath, dirnames, filenames in os.walk(item_path):
                                    for f in filenames:
                                        try:
                                            cleaned_size += os.path.getsize(os.path.join(dirpath, f))
                                        except:
                                            pass
                                shutil.rmtree(item_path)
                            else:
                                cleaned_size += os.path.getsize(item_path)
                                os.unlink(item_path)
                            cleaned_count += 1
                    except:
                        pass
            
            if cleaned_count > 0:
                size_mb = cleaned_size / (1024 * 1024)
                print(f"🧹 PERIODIC CLEANUP: {cleaned_count} old items deleted ({size_mb:.1f} MB)")
                
        except Exception as e:
            print(f"⚠️ Periodic cleanup error: {e}")

# Start periodic cleanup thread
cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
cleanup_thread.start()

# Log cleanup configuration
print(f"")
print(f"🔧 ════════════════════════════════════════════════")
print(f"🔧 STORAGE MANAGEMENT SETTINGS:")
print(f"   CLEANUP_ON_START: {CLEANUP_ON_START}")
print(f"   DELETE_AFTER_DOWNLOAD: {DELETE_AFTER_DOWNLOAD}")
print(f"   DELETION_DELAY_MINUTES: {DELETION_DELAY_MINUTES}min (after /confirm_download)")
print(f"   MAX_FILE_AGE_HOURS: {MAX_FILE_AGE_HOURS}h (periodic cleanup)")
print(f"   CLEANUP_INTERVAL_MINUTES: {CLEANUP_INTERVAL_MINUTES}min")
print(f"🔧 ════════════════════════════════════════════════")
print(f"")

# Configuration for retry logic
MAX_RETRY_ATTEMPTS = int(os.environ.get('MAX_RETRY_ATTEMPTS', 3))
RETRY_DELAY_SECONDS = int(os.environ.get('RETRY_DELAY_SECONDS', 5))

def process_track_without_separation(filepath, filename, track_type, session_id='global', worker_id=None):
    """
    Process a track WITHOUT running Demucs separation.
    Used for tracks with Instrumental/Extended/Acapella in the title.
    Exports only the Main version with the detected type as suffix.
    
    Returns: (success: bool, error_message: str or None)
    """
    current_status = get_job_status(session_id)
    
    try:
        current_status['state'] = 'processing'
        current_status['current_filename'] = filename
        current_status['current_step'] = f"Export direct ({track_type})..."
        
        log_message(f"⏭️ [{session_id}] Skip Demucs pour: {filename} (Type détecté: {track_type})", session_id)
        update_queue_item(filename, status='processing', worker=worker_id, progress=10, step=f'Export direct ({track_type})...')
        
        # Get original title from metadata
        try:
            original_audio = MP3(filepath, ID3=ID3)
            original_tags = original_audio.tags
            original_title = None
            if original_tags and 'TIT2' in original_tags:
                original_title = str(original_tags['TIT2'].text[0]) if original_tags['TIT2'].text else None
        except:
            original_tags = None
            original_title = None
        
        # Determine base name
        fallback_name, _ = clean_filename(filename)
        if original_title:
            metadata_base_name = original_title
            metadata_base_name = re.sub(r'[<>:"/\\|?*]', '', metadata_base_name)
            metadata_base_name = metadata_base_name.strip()
        else:
            metadata_base_name = fallback_name
        
        # Get BPM from original metadata
        bpm = None
        try:
            if original_tags and 'TBPM' in original_tags:
                bpm_text = str(original_tags['TBPM'].text[0]).strip()
                if bpm_text:
                    bpm = int(float(bpm_text))
        except:
            pass
        
        # Create output directory
        track_output_dir = os.path.join(PROCESSED_FOLDER, metadata_base_name)
        os.makedirs(track_output_dir, exist_ok=True)
        
        update_queue_item(filename, progress=30, step='Chargement audio...')
        
        # Load original audio
        original = AudioSegment.from_mp3(filepath)
        
        update_queue_item(filename, progress=50, step='Export MP3/WAV...')
        
        # Export with the detected type as suffix
        suffix = track_type
        out_name_mp3 = f"{metadata_base_name} - {suffix}.mp3"
        out_name_wav = f"{metadata_base_name} - {suffix}.wav"
        
        out_path_mp3 = os.path.join(track_output_dir, out_name_mp3)
        out_path_wav = os.path.join(track_output_dir, out_name_wav)
        
        metadata_title = f"{metadata_base_name} - {suffix}"
        
        # Export both formats
        from concurrent.futures import ThreadPoolExecutor
        
        def export_mp3():
            original.export(out_path_mp3, format="mp3", bitrate="320k")
            update_metadata(out_path_mp3, "ID By Rivoli", metadata_title, filepath, bpm)
        
        def export_wav():
            original.export(out_path_wav, format="wav")
            update_metadata_wav(out_path_wav, "ID By Rivoli", metadata_title, filepath, bpm)
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(export_mp3), executor.submit(export_wav)]
            for f in futures:
                f.result()
        
        update_queue_item(filename, progress=80, step='Envoi API...')
        
        # Build URLs
        rel_path_mp3 = f"{metadata_base_name}/{out_name_mp3}"
        rel_path_wav = f"{metadata_base_name}/{out_name_wav}"
        
        mp3_url = f"/download_file?path={urllib.parse.quote(rel_path_mp3, safe='/')}"
        wav_url = f"/download_file?path={urllib.parse.quote(rel_path_wav, safe='/')}"
        
        # Log URLs
        base_url = CURRENT_HOST_URL if CURRENT_HOST_URL else "http://localhost:8888"
        log_message(f"📥 URL MP3: {base_url}{mp3_url}")
        log_message(f"📥 URL WAV: {base_url}{wav_url}")
        
        # Prepare and send track info to API
        track_info_mp3 = {
            'type': suffix,
            'format': 'MP3',
            'name': metadata_title,
            'url': mp3_url
        }
        track_data_mp3 = prepare_track_metadata(track_info_mp3, filepath, bpm)
        if track_data_mp3:
            send_track_info_to_api(track_data_mp3)
        
        track_info_wav = {
            'type': suffix,
            'format': 'WAV',
            'name': metadata_title,
            'url': wav_url
        }
        track_data_wav = prepare_track_metadata(track_info_wav, filepath, bpm)
        if track_data_wav:
            send_track_info_to_api(track_data_wav)
        
        # Build edit result
        edit = {
            'name': metadata_title,
            'mp3': mp3_url,
            'wav': wav_url
        }
        
        # Register for pending download with file list for sequential tracking
        file_list = [f"{metadata_title}.mp3", f"{metadata_title}.wav"]
        track_file_for_pending_download(metadata_base_name, filepath, 2, file_list)
        
        update_queue_item(filename, progress=100, step=f'Terminé ({track_type}) ✅')
        
        # Add to session results
        current_status['results'].append({
            'original': metadata_base_name,
            'edits': [edit]
        })
        
        # Update upload history
        update_upload_history_status(filename, 'completed', track_type=track_type)
        
        log_message(f"✅ [{session_id}] Export direct terminé: {metadata_base_name} ({track_type})", session_id)
        
        current_status['progress'] = 100
        return True, None
        
    except Exception as e:
        error_msg = f"Erreur export direct: {str(e)}"
        log_message(f"❌ {error_msg} pour {filename}", session_id)
        import traceback
        traceback.print_exc()
        update_upload_history_status(filename, 'failed', error=error_msg)
        return False, error_msg

def add_failed_file(session_id, filename, error_message, filepath=None):
    """Add a file to the failed files list for a session."""
    current_status = get_job_status(session_id)
    
    # Check if already in failed list
    for failed in current_status['failed_files']:
        if failed['filename'] == filename:
            failed['error'] = error_message
            failed['retry_count'] = current_status['retry_count'].get(filename, 0)
            failed['timestamp'] = time.time()
            return
    
    # Add new failed entry
    current_status['failed_files'].append({
        'filename': filename,
        'filepath': filepath,
        'error': error_message,
        'retry_count': current_status['retry_count'].get(filename, 0),
        'timestamp': time.time()
    })
    log_message(f"❌ [{session_id}] Fichier ajouté aux échecs : {filename} - {error_message}", session_id)

def remove_failed_file(session_id, filename):
    """Remove a file from the failed files list (e.g., after successful retry)."""
    current_status = get_job_status(session_id)
    current_status['failed_files'] = [f for f in current_status['failed_files'] if f['filename'] != filename]

# Modified process function for SINGLE track with RETRY LOGIC
def process_single_track(filepath, filename, session_id='global', worker_id=None, is_retry=False):
    """
    Process a single track with comprehensive retry logic.
    Returns: (success: bool, error_message: str or None)
    """
    # Get session-specific status
    current_status = get_job_status(session_id)
    
    # Track retry attempts
    if filename not in current_status['retry_count']:
        current_status['retry_count'][filename] = 0
    
    # =============================================================================
    # CHECK FOR SKIP-ANALYSIS TRACK TYPES (Instrumental/Extended/Acapella)
    # =============================================================================
    try:
        original_audio = MP3(filepath, ID3=ID3)
        original_tags = original_audio.tags
        title_for_detection = None
        
        # Try to get title from metadata
        if original_tags and 'TIT2' in original_tags:
            title_for_detection = str(original_tags['TIT2'].text[0]) if original_tags['TIT2'].text else None
        
        # Fallback to filename if no title in metadata
        if not title_for_detection:
            title_for_detection = filename
        
        # Check if track type can be detected from title
        detected_type = detect_track_type_from_title(title_for_detection)
        
        if detected_type:
            log_message(f"⏭️ [{session_id}] Type détecté dans le titre: '{detected_type}' → Skip Demucs", session_id)
            # Add to upload history with detected type
            add_to_upload_history(filename, session_id, 'processing', detected_type)
            
            # Process without Demucs separation
            success, error = process_track_without_separation(filepath, filename, detected_type, session_id, worker_id)
            
            if success:
                remove_failed_file(session_id, filename)
            else:
                add_failed_file(session_id, filename, error, filepath)
                update_queue_item(filename, status='failed', progress=0, step=f'❌ {error[:50]}...')
            
            return success, error
            
    except Exception as e:
        log_message(f"⚠️ Erreur détection type: {e} - Traitement normal", session_id)
    
    # =============================================================================
    # NORMAL PROCESSING WITH DEMUCS
    # =============================================================================
    
    # Ensure CUDA is available (re-check in case it wasn't ready at startup)
    device = ensure_cuda_device()
    
    attempt = 0
    max_attempts = MAX_RETRY_ATTEMPTS
    last_error = None
    
    while attempt < max_attempts:
        attempt += 1
        retry_label = f" (Tentative {attempt}/{max_attempts})" if attempt > 1 or is_retry else ""
        
        try:
            current_status['state'] = 'processing'
            current_status['current_filename'] = filename
            current_status['current_step'] = f"Séparation IA (Demucs)...{retry_label}"
            log_message(f"🚀 [{session_id}] Début traitement : {filename} (Device: {device}){retry_label}", session_id)
            
            # Update queue tracker
            update_queue_item(filename, status='processing', worker=worker_id, progress=0, step=f'Séparation IA ({device})...{retry_label}')
            
            track_name = os.path.splitext(filename)[0]
            
            # 1. Run Demucs separation (OPTIMIZED FOR H100 80GB + 240GB RAM)
            def run_demucs_with_device(device):
                device_emoji = "🚀 GPU" if device == 'cuda' else "💻 CPU"
                log_message(f"🎵 Séparation vocale/instrumentale ({device_emoji})...")
                
                # H100 80GB + 240GB RAM optimizations:
                # - Maximum -j jobs (GPU has plenty of VRAM)
                # - Max segment size for htdemucs
                # - Minimal overlap for maximum speed
                if device == 'cuda':
                    try:
                        import torch
                        import psutil
                        gpu_mem_gb = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                        ram_gb = psutil.virtual_memory().total / (1024**3)
                        
                        # H100 + 240GB RAM: Maximum parallelism
                        if gpu_mem_gb >= 70 and ram_gb >= 200:
                            jobs = 20  # Maximum for H100 with high RAM
                        elif gpu_mem_gb >= 70:
                            jobs = 16
                        elif gpu_mem_gb >= 40:
                            jobs = 12
                        else:
                            jobs = 8
                    except:
                        jobs = 8
                else:
                    jobs = max(4, CPU_COUNT)
                
                cmd = [
                    'python3', '-m', 'demucs',
                    '--two-stems=vocals',
                    '-n', 'htdemucs',
                    '--mp3',
                    '--mp3-bitrate', '320',
                    '-j', str(jobs),
                    '--segment', '7',              # Max segment size (integer)
                    '--overlap', '0.1',            # Minimal overlap for speed
                    '--device', device,
                    '-o', OUTPUT_FOLDER,
                    filepath
                ]
                
                # Log the exact command for debugging
                cmd_str = ' '.join(cmd)
                print(f"🔧 DEMUCS COMMAND: {cmd_str}")
                log_message(f"🔧 Device: {device}, Jobs: {jobs}")
                
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                output_lines = []
                for line in proc.stdout:
                    print(line, end='')
                    output_lines.append(line)
                    
                    # Check for CUDA/GPU related messages
                    line_lower = line.lower()
                    if 'cuda' in line_lower or 'gpu' in line_lower or 'cpu' in line_lower:
                        log_message(f"🔍 Demucs: {line.strip()}")
                    
                    if "%|" in line:
                        try:
                            parts = line.split('%|')
                            if len(parts) > 0:
                                percent_part = parts[0].strip()
                                p_match = re.search(r'(\d+)$', percent_part)
                                if p_match:
                                    track_percent = int(p_match.group(1))
                                    current_status['progress'] = int(track_percent * 0.7)
                                    # Update queue tracker with progress
                                    update_queue_item(filename, progress=int(track_percent * 0.7), step=f'Séparation IA {track_percent}%{retry_label}')
                        except:
                            pass
                
                proc.wait()
                return proc.returncode, output_lines
            
            # Try with detected device first
            returncode, demucs_output = run_demucs_with_device(DEMUCS_DEVICE)
            
            # If GPU failed, fallback to CPU
            if returncode != 0 and DEMUCS_DEVICE == 'cuda':
                log_message(f"⚠️ GPU échoué, fallback vers CPU...")
                returncode, demucs_output = run_demucs_with_device('cpu')
            
            if returncode != 0:
                error_lines = ''.join(demucs_output[-10:])
                error_msg = f"Erreur Demucs (code {returncode}): {error_lines[:200]}"
                log_message(f"❌ {error_msg}", session_id)
                print(f"DEMUCS ERROR OUTPUT:\n{error_lines}")
                last_error = error_msg
                
                # Wait before retry
                if attempt < max_attempts:
                    log_message(f"⏳ Attente {RETRY_DELAY_SECONDS}s avant nouvelle tentative...", session_id)
                    time.sleep(RETRY_DELAY_SECONDS)
                continue  # Retry
            
            # 2. Generate edits (Main, Acapella, Instrumental)
            current_status['current_step'] = f"Génération des versions...{retry_label}"
            current_status['progress'] = 70
            update_queue_item(filename, progress=70, step=f'Export MP3/WAV...{retry_label}')
            
            clean_name, _ = clean_filename(filename)
            track_output_dir = os.path.join(PROCESSED_FOLDER, clean_name)
            os.makedirs(track_output_dir, exist_ok=True)
            
            # Get separated files
            source_dir = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
            inst_path = os.path.join(source_dir, 'no_vocals.mp3')
            vocals_path = os.path.join(source_dir, 'vocals.mp3')
            
            # Check if separated files exist with retry
            files_found = False
            for wait_attempt in range(5):
                if os.path.exists(inst_path) and os.path.exists(vocals_path):
                    files_found = True
                    break
                if wait_attempt < 4:
                    log_message(f"⏳ Attente des fichiers séparés ({wait_attempt + 1}/5)...", session_id)
                    time.sleep(2)
            
            if not files_found:
                error_msg = f"Fichiers séparés non trouvés après Demucs: {source_dir}"
                log_message(f"❌ {error_msg}", session_id)
                last_error = error_msg
                
                if attempt < max_attempts:
                    log_message(f"⏳ Attente {RETRY_DELAY_SECONDS}s avant nouvelle tentative...", session_id)
                    time.sleep(RETRY_DELAY_SECONDS)
                continue  # Retry
            
            # Try creating edits with error handling
            update_queue_item(filename, progress=80, step=f'Création des versions...{retry_label}')
            try:
                edits = create_edits(vocals_path, inst_path, filepath, track_output_dir, filename)
            except Exception as edit_error:
                error_msg = f"Erreur création édits: {str(edit_error)}"
                log_message(f"❌ {error_msg}", session_id)
                last_error = error_msg
                
                if attempt < max_attempts:
                    log_message(f"⏳ Attente {RETRY_DELAY_SECONDS}s avant nouvelle tentative...", session_id)
                    time.sleep(RETRY_DELAY_SECONDS)
                continue  # Retry
            
            update_queue_item(filename, progress=100, step='Terminé ✅')
            
            # Add to session-specific results
            current_status['results'].append({
                'original': clean_name,
                'edits': edits
            })
            
            # Remove from failed files if it was there (successful retry)
            remove_failed_file(session_id, filename)
            
            # Update retry count
            current_status['retry_count'][filename] = attempt
            
            # Update upload history with completed status
            update_upload_history_status(filename, 'completed', track_type='Full Analysis')
            
            log_message(f"✅ [{session_id}] Terminé : {clean_name}" + (f" (après {attempt} tentative(s))" if attempt > 1 else ""), session_id)
            
            current_status['progress'] = 100
            return True, None  # Success

        except Exception as e:
            error_msg = f"Erreur critique: {str(e)}"
            log_message(f"❌ {error_msg} pour {filename}", session_id)
            import traceback
            traceback.print_exc()
            last_error = error_msg
            
            if attempt < max_attempts:
                log_message(f"⏳ Attente {RETRY_DELAY_SECONDS}s avant nouvelle tentative...", session_id)
                time.sleep(RETRY_DELAY_SECONDS)
            continue  # Retry
    
    # All attempts failed
    final_error = f"Échec après {max_attempts} tentatives. Dernière erreur: {last_error}"
    log_message(f"❌ [{session_id}] ÉCHEC DÉFINITIF pour {filename}: {final_error}", session_id)
    
    # Track the failed file
    add_failed_file(session_id, filename, final_error, filepath)
    current_status['retry_count'][filename] = max_attempts
    
    # Update upload history with failed status
    update_upload_history_status(filename, 'failed', error=final_error)
    
    # Update UI to show failure
    update_queue_item(filename, status='failed', progress=0, step=f'❌ Échec: {last_error[:50]}...')
    
    return False, final_error

@app.route('/clear_results', methods=['POST'])
def clear_results():
    """Clears only the results list for current session (keeps files on disk)."""
    session_id = get_session_id()
    current_status = get_job_status(session_id)
    current_status['results'] = []
    current_status['logs'] = []
    log_message("🔄 Résultats vidés - prêt pour nouveaux tracks", session_id)
    return jsonify({'message': 'Results cleared', 'session_id': session_id})

@app.route('/enqueue_file', methods=['POST'])
def enqueue_file():
    data = request.json
    filename = data.get('filename')
    session_id = get_session_id()
    force_reprocess = data.get('force', False)  # Optional flag to force reprocessing
    
    # Check pending downloads warning
    pending_warning = check_pending_tracks_warning()
    
    # Block new uploads if we've reached the critical limit
    if pending_warning.get('level') == 'critical':
        log_message(f"⚠️ [{session_id}] Upload bloqué: trop de tracks en attente ({pending_warning['count']})", session_id)
        return jsonify({
            'error': 'Too many pending downloads',
            'warning': pending_warning,
            'message': pending_warning['message']
        }), 429  # Too Many Requests
    
    if filename:
        # =============================================================================
        # CHECK IF TRACK ALREADY PROCESSED - SKIP IF YES
        # =============================================================================
        if not force_reprocess:
            is_processed, processed_dir = is_track_already_processed(filename)
            if is_processed:
                clean_name, _ = clean_filename(filename)
                log_message(f"⏭️ [{session_id}] Track déjà traité, skip: {filename} → {clean_name}", session_id)
                
                # Update upload history with skipped status
                add_to_upload_history(filename, session_id, 'skipped', 'already_processed')
                
                return jsonify({
                    'message': 'Skipped - already processed',
                    'skipped': True,
                    'track_name': clean_name,
                    'processed_dir': processed_dir,
                    'session_id': session_id,
                    'pending_downloads': get_pending_tracks_count()
                })
        # Verify file exists before queueing (with brief retry for race condition)
        session_upload_folder = os.path.join(UPLOAD_FOLDER, session_id)
        filepath = os.path.join(session_upload_folder, filename)
        
        # Check if file exists, retry a few times if not (upload might still be completing)
        file_exists = os.path.exists(filepath)
        if not file_exists:
            # Also check global folder
            filepath_global = os.path.join(UPLOAD_FOLDER, filename)
            file_exists = os.path.exists(filepath_global)
        
        if not file_exists:
            # Brief retry (max 3 seconds) for slow uploads
            for retry in range(3):
                time.sleep(1)
                if os.path.exists(filepath) or os.path.exists(os.path.join(UPLOAD_FOLDER, filename)):
                    file_exists = True
                    print(f"   ✅ File confirmed after {retry + 1}s wait: {filename}")
                    break
        
        if not file_exists:
            log_message(f"⚠️ [{session_id}] Fichier non trouvé, impossible d'ajouter à la file : {filename}", session_id)
            return jsonify({'error': 'File not found', 'filename': filename}), 404
        
        # Add to queue tracker for UI display
        add_to_queue_tracker(filename, session_id)
        
        # Update upload history status to 'processing'
        update_upload_history_status(filename, 'processing')
        
        # Queue item includes session_id for multi-user support
        track_queue.put({'filename': filename, 'session_id': session_id})
        q_size = track_queue.qsize()
        log_message(f"📥 [{session_id}] Ajouté à la file : {filename} (File d'attente: {q_size})", session_id)
        
        # Include warning if there are many pending downloads
        response = {
            'message': 'Queued', 
            'queue_size': q_size, 
            'session_id': session_id,
            'pending_downloads': get_pending_tracks_count()
        }
        if pending_warning.get('warning'):
            response['pending_warning'] = pending_warning
            log_message(f"⚠️ [{session_id}] Avertissement: {pending_warning['message']}", session_id)
        
        return jsonify(response)
    
    return jsonify({'error': 'No filename'}), 400

@app.route('/upload_chunk', methods=['POST'])
def upload_chunk():
    """
    Receives a single file upload and saves it to session-specific folder.
    Supports multiple users uploading simultaneously.
    Handles folder uploads where filename may contain path (e.g., "Folder/file.mp3").
    Uses semaphore to limit concurrent uploads (supports 1000+ track batch uploads).
    
    Optional form parameter:
    - auto_enqueue: If 'true', automatically adds file to processing queue after upload.
                   This enables parallel upload + analysis for better storage efficiency.
    - force_reprocess: If 'true', reprocess even if track was already processed.
    """
    # Acquire semaphore with timeout to prevent deadlocks
    acquired = UPLOAD_SEMAPHORE.acquire(timeout=300)  # 5 minute timeout
    if not acquired:
        return jsonify({'error': 'Server busy, too many concurrent uploads. Please retry.'}), 503
    
    try:
        session_id = get_session_id()
        
        # Check for auto_enqueue option (enables upload + analyze in parallel)
        auto_enqueue = request.form.get('auto_enqueue', 'false').lower() == 'true'
        force_reprocess = request.form.get('force_reprocess', 'false').lower() == 'true'
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
            
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
            
        if file:
            # Get the original filename (may contain folder path for folder uploads)
            original_filename = file.filename
            
            # Extract just the basename (remove folder path if present)
            # This handles both "Folder/Subfolder/file.mp3" and just "file.mp3"
            # Works with both / (Unix) and \ (Windows) paths
            safe_filename = os.path.basename(original_filename.replace('\\', '/'))
            
            # Remove any remaining problematic characters
            safe_filename = safe_filename.replace('\0', '')
            
            # If filename is empty after processing, generate a unique one
            if not safe_filename:
                safe_filename = f"upload_{uuid.uuid4().hex[:8]}.mp3"
            
            print(f"📤 Upload: '{original_filename}' → '{safe_filename}'" + (" [AUTO-ENQUEUE]" if auto_enqueue else ""))
            
            # Use session-specific upload folder
            session_upload_folder = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
            os.makedirs(session_upload_folder, exist_ok=True)
            filepath = os.path.join(session_upload_folder, safe_filename)
            
            # Save file with explicit error handling
            try:
                file.save(filepath)
                print(f"✅ Saved: {safe_filename} ({os.path.getsize(filepath)} bytes)")
                
                # Add to upload history
                add_to_upload_history(safe_filename, session_id, 'uploaded', 'Pending')
                
            except Exception as save_error:
                print(f"❌ Save error for {safe_filename}: {save_error}")
                import traceback
                traceback.print_exc()
                return jsonify({'error': f'Failed to save file: {str(save_error)}'}), 500
            
            # Build response
            response_data = {
                'message': f'File {safe_filename} uploaded successfully', 
                'session_id': session_id,
                'filename': safe_filename,
                'original_filename': original_filename,
                'auto_enqueued': False
            }
            
            # =========================================================================
            # AUTO-ENQUEUE: Immediately add to processing queue if requested
            # This enables parallel upload + analysis for better storage efficiency
            # =========================================================================
            if auto_enqueue:
                # Check pending downloads warning
                pending_warning = check_pending_tracks_warning()
                
                # Block if we've reached critical limit
                if pending_warning.get('level') == 'critical':
                    log_message(f"⚠️ [{session_id}] Auto-enqueue bloqué: trop de tracks en attente ({pending_warning['count']})", session_id)
                    response_data['auto_enqueue_blocked'] = True
                    response_data['pending_warning'] = pending_warning
                    return jsonify(response_data)
                
                # Check if track already processed (skip if yes, unless force_reprocess)
                if not force_reprocess:
                    is_processed, processed_dir = is_track_already_processed(safe_filename)
                    if is_processed:
                        clean_name, _ = clean_filename(safe_filename)
                        log_message(f"⏭️ [{session_id}] Track déjà traité, skip auto-enqueue: {safe_filename} → {clean_name}", session_id)
                        
                        # Update upload history with skipped status
                        add_to_upload_history(safe_filename, session_id, 'skipped', 'already_processed')
                        
                        # Clean up the uploaded file since it's already processed
                        try:
                            os.remove(filepath)
                            print(f"🗑️ Removed duplicate upload: {safe_filename}")
                        except Exception as e:
                            print(f"⚠️ Could not remove duplicate: {e}")
                        
                        response_data['skipped'] = True
                        response_data['track_name'] = clean_name
                        response_data['processed_dir'] = processed_dir
                        response_data['pending_downloads'] = get_pending_tracks_count()
                        return jsonify(response_data)
                
                # Add to queue tracker for UI display
                add_to_queue_tracker(safe_filename, session_id)
                
                # Update upload history status to 'processing'
                update_upload_history_status(safe_filename, 'processing')
                
                # Queue item includes session_id for multi-user support
                track_queue.put({'filename': safe_filename, 'session_id': session_id})
                q_size = track_queue.qsize()
                log_message(f"📥 [{session_id}] Auto-enqueued: {safe_filename} (Queue: {q_size})", session_id)
                
                response_data['auto_enqueued'] = True
                response_data['queue_size'] = q_size
                response_data['pending_downloads'] = get_pending_tracks_count()
                
                if pending_warning.get('warning'):
                    response_data['pending_warning'] = pending_warning
            
            return jsonify(response_data)
        
        return jsonify({'error': 'No file provided'}), 400
        
    except Exception as e:
        print(f"❌ Upload endpoint error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Upload error: {str(e)}'}), 500
    finally:
        UPLOAD_SEMAPHORE.release()

@app.route('/start_processing', methods=['POST'])
def start_processing():
    """
    Triggered after all uploads are done.
    Scans the uploads folder and starts the processing thread.
    """
    global job_status
    
    if job_status['state'] == 'processing':
        return jsonify({'error': 'Un traitement est déjà en cours. Veuillez patienter.'}), 409

    # Scan upload folder for MP3s
    files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.lower().endswith('.mp3')]
    
    if not files:
        return jsonify({'error': 'Aucun fichier trouvé dans le dossier uploads'}), 400

    saved_filepaths = [os.path.join(app.config['UPLOAD_FOLDER'], f) for f in files]
    original_filenames = files # filenames are just the basenames
    
    job_status = {
        'state': 'starting',
        'progress': 0,
        'total_files': len(files),
        'current_file_idx': 0,
        'current_filename': '',
        'current_step': 'Initialisation...',
        'results': [],
        'error': None,
        'logs': []
    }
    
    log_message(f"Traitement démarré pour {len(files)} fichier(s) (Mode Batch)")
    
    thread = threading.Thread(target=run_demucs_thread, args=(saved_filepaths, original_filenames))
    thread.start()
    
    return jsonify({'message': 'Traitement démarré', 'total_files': len(files)})

@app.route('/upload', methods=['POST'])
def upload_file():
    # Keep legacy endpoint for backward compatibility if needed, 
    # but strictly we should move to the new flow.
    # ... (redirecting to new logic ideally, but let's keep it simple)
    return jsonify({'error': 'Please use the new sequential upload flow'}), 400


# =============================================================================
# UPLOAD ONLY MODE - Direct upload to S3 + Database (no processing)
# =============================================================================

@app.route('/upload_direct', methods=['POST'])
def upload_direct():
    """
    Upload a track directly to S3 and create database entry WITHOUT audio processing.
    
    This workflow:
    1. Receives an MP3 file
    2. Reads metadata from the MP3 (title, artist, album, BPM, etc.)
    3. Uploads the file to S3
    4. Creates/updates the track in the database via Prisma
    
    No Demucs separation, no edits, no WAV conversion - just direct upload.
    
    Form parameters:
    - file: The MP3 file
    - track_type: Optional override for type (auto-detected from title if not provided)
    - format: 'MP3' or 'WAV' (default: auto-detected from file)
    - skip_waveform: If 'true', skip waveform generation for faster uploads
    """
    if not USE_DATABASE_MODE:
        return jsonify({'error': 'Direct upload requires database mode to be enabled'}), 400
    
    session_id = get_session_id()
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    # Track type will be auto-detected from title, but can be overridden
    manual_track_type = request.form.get('track_type', None)
    
    # Skip waveform generation for faster uploads
    skip_waveform = request.form.get('skip_waveform', 'false').lower() == 'true'
    
    # Detect format from file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    format_type = 'WAV' if file_ext == '.wav' else 'MP3'
    
    # Override format if explicitly provided
    if request.form.get('format'):
        format_type = request.form.get('format').upper()
    
    try:
        # Save file temporarily
        temp_dir = os.path.join(UPLOAD_FOLDER, session_id, 'direct_upload')
        os.makedirs(temp_dir, exist_ok=True)
        
        safe_filename = re.sub(r'[^\w\s\-\.]', '', file.filename)
        safe_filename = safe_filename.strip() or 'track.mp3'
        temp_path = os.path.join(temp_dir, safe_filename)
        
        file.save(temp_path)
        log_message(f"📁 [{session_id}] Direct upload saved: {safe_filename}")
        
        # Extract metadata from the file
        bpm = None
        original_title = None
        artist = 'Unknown'
        album = ''
        genre = ''
        isrc = ''
        date_sortie = 0
        sous_label = ''
        parent_label = ''
        cover_url = ''
        
        try:
            if format_type == 'MP3':
                original_audio = MP3(temp_path, ID3=ID3)
                original_tags = original_audio.tags if original_audio.tags else {}
                
                # Title
                if 'TIT2' in original_tags and original_tags['TIT2'].text:
                    original_title = str(original_tags['TIT2'].text[0]).strip()
                
                # Artist
                if 'TPE1' in original_tags:
                    artist_raw = str(original_tags['TPE1']).strip()
                    artist = format_artists(artist_raw)
                
                # Album
                if 'TALB' in original_tags:
                    album = str(original_tags['TALB']).strip()
                
                # Genre/Style
                if 'TCON' in original_tags:
                    genre = str(original_tags['TCON']).strip()
                
                # BPM
                if 'TBPM' in original_tags and original_tags['TBPM'].text:
                    try:
                        bpm_text = str(original_tags['TBPM'].text[0]).strip()
                        if bpm_text:
                            bpm = int(float(bpm_text))
                    except:
                        pass
                
                # ISRC
                if 'TSRC' in original_tags and original_tags['TSRC'].text:
                    isrc = str(original_tags['TSRC'].text[0]).strip()
                
                # Release date
                if 'TDRC' in original_tags:
                    date_str = str(original_tags['TDRC']).strip()
                    try:
                        if date_str:
                            date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                            date_sortie = int(date_obj.timestamp())
                    except:
                        pass
                
                # Publisher/Label
                if 'TPUB' in original_tags and original_tags['TPUB'].text:
                    sous_label = str(original_tags['TPUB'].text[0]).strip()
                    parent_label = get_parent_label(sous_label) if sous_label else ''
                    if parent_label == sous_label:
                        parent_label = ''
                
                # Extract cover image
                for apic_key in original_tags.keys():
                    if apic_key.startswith('APIC'):
                        try:
                            original_apic = original_tags[apic_key]
                            apic_desc = getattr(original_apic, 'desc', '')
                            if 'ID By Rivoli' in str(apic_desc):
                                continue
                            
                            track_name_clean = re.sub(r'[^\w\s-]', '', os.path.splitext(safe_filename)[0])
                            track_name_clean = track_name_clean.replace(' ', '_')[:50]
                            
                            mime = getattr(original_apic, 'mime', 'image/jpeg')
                            ext = 'jpg' if 'jpeg' in mime.lower() else 'png'
                            cover_filename = f"cover_{track_name_clean}.{ext}"
                            cover_save_path = os.path.join(BASE_DIR, 'static', 'covers', cover_filename)
                            
                            with open(cover_save_path, 'wb') as f:
                                f.write(original_apic.data)
                            
                            base_url = CURRENT_HOST_URL if CURRENT_HOST_URL else ""
                            cover_url = f"{base_url}/static/covers/{cover_filename}"
                            break
                        except:
                            pass
            
            elif format_type == 'WAV':
                # For WAV files, try to get info from filename or use defaults
                try:
                    import wave
                    with wave.open(temp_path, 'rb') as wav_file:
                        # Basic WAV info available, but no metadata
                        pass
                except:
                    pass
                
        except Exception as e:
            log_message(f"⚠️ [{session_id}] Metadata extraction warning: {e}")
        
        # Use filename as fallback title
        if not original_title:
            original_title = os.path.splitext(safe_filename)[0]
        
        # Auto-detect track type from title/filename
        detected_type = detect_track_type_from_title(original_title)
        if not detected_type:
            # Also check filename if title didn't have type
            detected_type = detect_track_type_from_title(safe_filename)
        
        # Use manual override if provided, otherwise use detected type, default to 'Main'
        if manual_track_type:
            track_type = manual_track_type
        elif detected_type:
            track_type = detected_type
        else:
            track_type = 'Main'
        
        log_message(f"🔍 [{session_id}] Track type: {track_type} (detected: {detected_type}, manual: {manual_track_type})")
        
        # Build the track title
        # If the type is already in the title, use the title as-is
        # Otherwise, append the type suffix
        title_lower = original_title.lower()
        type_already_in_title = (
            track_type.lower() in title_lower or
            (track_type == 'Acapella' and ('acapella' in title_lower or 'a capella' in title_lower or 'acappella' in title_lower))
        )
        
        if type_already_in_title:
            # Title already contains the type, use as-is
            track_title = original_title
        elif track_type.lower() != 'main':
            # Add type suffix for non-Main types
            track_title = f"{original_title} - {track_type}"
        else:
            # Main type - add suffix
            track_title = f"{original_title} - Main"
        
        # Create relative URL for the file (will be uploaded to S3 by database_service)
        rel_path = f"direct_upload/{safe_filename}"
        file_url = f"/download_file?path={urllib.parse.quote(rel_path, safe='/')}"
        base_url = CURRENT_HOST_URL if CURRENT_HOST_URL else ""
        absolute_url = f"{base_url}{file_url}"
        
        # Move file to processed folder for serving
        processed_dir = os.path.join(PROCESSED_FOLDER, 'direct_upload')
        os.makedirs(processed_dir, exist_ok=True)
        final_path = os.path.join(processed_dir, safe_filename)
        
        if os.path.exists(final_path):
            os.remove(final_path)
        shutil.move(temp_path, final_path)
        
        # Generate track ID
        filename_clean = original_title.replace('-', ' ').replace('_', ' ')
        filename_clean = re.sub(r'\s+', ' ', filename_clean).strip()
        filename_clean = filename_clean.replace(' ', '_')
        filename_clean = re.sub(r'_+', '_', filename_clean)
        track_id = f"{isrc}_{filename_clean}" if isrc else filename_clean
        
        # Prepare track data for database
        track_data = {
            'Type': track_type,
            'Format': format_type,
            'Titre': track_title,
            'Artiste': artist,
            'Fichiers': absolute_url,
            'Univers': '',
            'Mood': '',
            'Style': genre,
            'Album': album,
            'Label': parent_label,
            'Sous-label': sous_label,
            'Date de sortie': date_sortie,
            'BPM': bpm if bpm is not None else 0,
            'Artiste original': artist,
            'Url': cover_url,
            'ISRC': isrc,
            'TRACK_ID': track_id
        }
        
        log_message(f"📤 [{session_id}] Sending to database: {track_title} ({format_type}){' [fast mode]' if skip_waveform else ''}")
        
        # Save to database (this handles S3 upload internally)
        result = save_track_to_database(track_data, skip_waveform=skip_waveform)
        
        if 'error' in result:
            log_message(f"❌ [{session_id}] Database error: {result['error']}")
            return jsonify({
                'success': False,
                'error': result['error'],
                'filename': safe_filename
            }), 500
        
        # Add to upload history
        add_to_upload_history(safe_filename, session_id, 'completed', track_type)
        
        log_message(f"✅ [{session_id}] Direct upload complete: {track_title}")
        
        return jsonify({
            'success': True,
            'message': 'Track uploaded and saved to database',
            'filename': safe_filename,
            'track_id': result.get('trackId', track_id),
            'database_id': result.get('id'),
            'action': result.get('action', 'created'),
            'type': track_type,
            'format': format_type,
            'title': track_title,
            'skip_waveform': skip_waveform
        })
        
    except Exception as e:
        log_message(f"❌ [{session_id}] Direct upload failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'filename': file.filename if file else 'unknown'
        }), 500
    finally:
        # Cleanup temp directory
        try:
            if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                os.rmdir(temp_dir)
        except:
            pass


@app.route('/upload_direct_batch', methods=['POST'])
def upload_direct_batch():
    """
    Batch upload multiple tracks directly to S3 + database.
    Accepts multiple files in a single request.
    Track type is auto-detected from each file's title.
    
    Form parameters:
    - files: Multiple MP3/WAV files
    - track_type: Optional fallback type if auto-detection fails (default: 'Main')
    - skip_waveform: If 'true', skip waveform generation for faster uploads
    """
    if not USE_DATABASE_MODE:
        return jsonify({'error': 'Direct upload requires database mode to be enabled'}), 400
    
    session_id = get_session_id()
    
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    
    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': 'No files selected'}), 400
    
    fallback_track_type = request.form.get('track_type', 'Main')
    skip_waveform = request.form.get('skip_waveform', 'false').lower() == 'true'
    
    results = []
    success_count = 0
    error_count = 0
    
    for file in files:
        if not file.filename:
            continue
        
        try:
            # Create a mock request-like structure for reuse
            # Save file temporarily
            temp_dir = os.path.join(UPLOAD_FOLDER, session_id, 'direct_upload_batch')
            os.makedirs(temp_dir, exist_ok=True)
            
            safe_filename = re.sub(r'[^\w\s\-\.]', '', file.filename)
            safe_filename = safe_filename.strip() or f'track_{len(results)}.mp3'
            temp_path = os.path.join(temp_dir, safe_filename)
            
            file.save(temp_path)
            
            # Detect format
            file_ext = os.path.splitext(file.filename)[1].lower()
            format_type = 'WAV' if file_ext == '.wav' else 'MP3'
            
            # Extract metadata (simplified version)
            original_title = os.path.splitext(safe_filename)[0]
            bpm = None
            artist = 'Unknown'
            album = ''
            genre = ''
            isrc = ''
            date_sortie = 0
            sous_label = ''
            parent_label = ''
            cover_url = ''
            
            if format_type == 'MP3':
                try:
                    original_audio = MP3(temp_path, ID3=ID3)
                    original_tags = original_audio.tags if original_audio.tags else {}
                    
                    if 'TIT2' in original_tags and original_tags['TIT2'].text:
                        original_title = str(original_tags['TIT2'].text[0]).strip()
                    if 'TPE1' in original_tags:
                        artist = format_artists(str(original_tags['TPE1']).strip())
                    if 'TALB' in original_tags:
                        album = str(original_tags['TALB']).strip()
                    if 'TCON' in original_tags:
                        genre = str(original_tags['TCON']).strip()
                    if 'TBPM' in original_tags and original_tags['TBPM'].text:
                        try:
                            bpm = int(float(str(original_tags['TBPM'].text[0]).strip()))
                        except:
                            pass
                    if 'TSRC' in original_tags and original_tags['TSRC'].text:
                        isrc = str(original_tags['TSRC'].text[0]).strip()
                    if 'TDRC' in original_tags:
                        try:
                            date_str = str(original_tags['TDRC']).strip()
                            if date_str:
                                date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                                date_sortie = int(date_obj.timestamp())
                        except:
                            pass
                    if 'TPUB' in original_tags and original_tags['TPUB'].text:
                        sous_label = str(original_tags['TPUB'].text[0]).strip()
                        parent_label = get_parent_label(sous_label) if sous_label else ''
                        if parent_label == sous_label:
                            parent_label = ''
                except:
                    pass
            
            # Auto-detect track type from title/filename
            detected_type = detect_track_type_from_title(original_title)
            if not detected_type:
                detected_type = detect_track_type_from_title(safe_filename)
            
            # Use detected type, or fallback to provided/default
            track_type = detected_type if detected_type else fallback_track_type
            
            # Build track title
            # If the type is already in the title, use as-is
            title_lower = original_title.lower()
            type_already_in_title = (
                track_type.lower() in title_lower or
                (track_type == 'Acapella' and ('acapella' in title_lower or 'a capella' in title_lower or 'acappella' in title_lower))
            )
            
            if type_already_in_title:
                track_title = original_title
            elif track_type.lower() != 'main':
                track_title = f"{original_title} - {track_type}"
            else:
                track_title = f"{original_title} - Main"
            
            # Move to processed folder
            processed_dir = os.path.join(PROCESSED_FOLDER, 'direct_upload')
            os.makedirs(processed_dir, exist_ok=True)
            final_path = os.path.join(processed_dir, safe_filename)
            
            if os.path.exists(final_path):
                os.remove(final_path)
            shutil.move(temp_path, final_path)
            
            # Build URL
            rel_path = f"direct_upload/{safe_filename}"
            file_url = f"/download_file?path={urllib.parse.quote(rel_path, safe='/')}"
            base_url = CURRENT_HOST_URL if CURRENT_HOST_URL else ""
            absolute_url = f"{base_url}{file_url}"
            
            # Generate track ID
            filename_clean = original_title.replace('-', ' ').replace('_', ' ')
            filename_clean = re.sub(r'\s+', ' ', filename_clean).strip()
            filename_clean = filename_clean.replace(' ', '_')
            filename_clean = re.sub(r'_+', '_', filename_clean)
            track_id = f"{isrc}_{filename_clean}" if isrc else filename_clean
            
            # Prepare track data
            track_data = {
                'Type': track_type,
                'Format': format_type,
                'Titre': track_title,
                'Artiste': artist,
                'Fichiers': absolute_url,
                'Univers': '',
                'Mood': '',
                'Style': genre,
                'Album': album,
                'Label': parent_label,
                'Sous-label': sous_label,
                'Date de sortie': date_sortie,
                'BPM': bpm if bpm is not None else 0,
                'Artiste original': artist,
                'Url': cover_url,
                'ISRC': isrc,
                'TRACK_ID': track_id
            }
            
            # Save to database
            result = save_track_to_database(track_data, skip_waveform=skip_waveform)
            
            if 'error' in result:
                results.append({
                    'filename': safe_filename,
                    'success': False,
                    'error': result['error']
                })
                error_count += 1
            else:
                results.append({
                    'filename': safe_filename,
                    'success': True,
                    'track_id': result.get('trackId', track_id),
                    'database_id': result.get('id'),
                    'action': result.get('action', 'created'),
                    'skip_waveform': skip_waveform
                })
                success_count += 1
                add_to_upload_history(safe_filename, session_id, 'completed', track_type)
                
        except Exception as e:
            results.append({
                'filename': file.filename,
                'success': False,
                'error': str(e)
            })
            error_count += 1
    
    # Cleanup temp directory
    try:
        temp_dir = os.path.join(UPLOAD_FOLDER, session_id, 'direct_upload_batch')
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    except:
        pass
    
    return jsonify({
        'success': error_count == 0,
        'total': len(results),
        'success_count': success_count,
        'error_count': error_count,
        'results': results
    })


# =============================================================================
# DROPBOX IMPORT - Fetch tracks from Dropbox folder
# =============================================================================

@app.route('/dropbox/list', methods=['POST'])
def dropbox_list_files():
    """
    List all audio files (MP3/WAV) in a Dropbox folder.
    Supports both personal folders and team shared folders.
    
    JSON body:
    - folder_path: Dropbox folder path (e.g., "/Music/Tracks" or "" for root)
    - namespace_id: Optional namespace ID for team folders
    
    Returns list of files with metadata.
    """
    # Reload .env in case token was added after startup
    load_dotenv(override=True)
    
    # Re-read token from environment
    dropbox_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    print(f"📦 Dropbox list request - Token configured: {bool(dropbox_token)}, Token length: {len(dropbox_token) if dropbox_token else 0}, Team member ID: {bool(dropbox_team_member_id)}")
    
    if not dropbox_token:
        return jsonify({'error': 'Dropbox not configured. Set DROPBOX_ACCESS_TOKEN in .env'}), 400
    
    data = request.json or {}
    folder_path = data.get('folder_path', '').strip()
    namespace_id = data.get('namespace_id', '')
    
    # Normalize path - Dropbox API expects empty string for root or path starting with /
    if folder_path and not folder_path.startswith('/'):
        folder_path = '/' + folder_path
    if folder_path == '/':
        folder_path = ''
    
    try:
        # Call Dropbox API to list folder contents
        headers = {
            'Authorization': f'Bearer {dropbox_token}',
            'Content-Type': 'application/json'
        }
        
        # Add team member header for Dropbox Business team tokens
        if dropbox_team_member_id:
            headers['Dropbox-API-Select-User'] = dropbox_team_member_id
        
        # AUTO-DETECT root namespace for team accounts if not provided
        # This is crucial for accessing content inside team folders
        if not namespace_id and dropbox_team_member_id:
            try:
                # Note: get_current_account requires no JSON body
                account_headers = {
                    'Authorization': f'Bearer {dropbox_token}',
                    'Dropbox-API-Select-User': dropbox_team_member_id
                }
                account_response = requests.post(
                    'https://api.dropboxapi.com/2/users/get_current_account',
                    headers=account_headers
                )
                if account_response.status_code == 200:
                    account_data = account_response.json()
                    root_info = account_data.get('root_info', {})
                    # Use root_namespace_id for team folders
                    root_ns = root_info.get('root_namespace_id')
                    if root_ns:
                        namespace_id = root_ns
                        print(f"📦 Auto-detected root namespace: {namespace_id}")
            except Exception as e:
                print(f"⚠️ Could not auto-detect namespace: {e}")
        
        print(f"📦 Dropbox folder path: '{folder_path}', namespace: '{namespace_id}'")
        
        # Add namespace header for team folders
        if namespace_id:
            headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})
        
        all_files = []
        has_more = True
        cursor = None
        
        while has_more:
            if cursor:
                # Continue listing
                response = requests.post(
                    'https://api.dropboxapi.com/2/files/list_folder/continue',
                    headers=headers,
                    json={'cursor': cursor}
                )
            else:
                # Initial listing - not recursive for browsing
                response = requests.post(
                    'https://api.dropboxapi.com/2/files/list_folder',
                    headers=headers,
                    json={
                        'path': folder_path,
                        'recursive': False,  # Browse one level at a time
                        'include_media_info': False,
                        'include_deleted': False
                    }
                )
            
            print(f"📦 Dropbox API response: status={response.status_code}, length={len(response.text) if response.text else 0}")
            
            if response.status_code != 200:
                print(f"❌ Dropbox API error response: {response.text[:500] if response.text else 'empty'}")
                try:
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('error_summary', response.text or 'Unknown error')
                except:
                    error_msg = response.text or f'HTTP {response.status_code}'
                return jsonify({'error': f'Dropbox API error: {error_msg}'}), response.status_code
            
            if not response.text:
                print("❌ Dropbox returned empty response")
                return jsonify({'error': 'Dropbox returned empty response - token may be expired'}), 500
            
            try:
                result = response.json()
            except Exception as json_err:
                print(f"❌ Failed to parse Dropbox response: {response.text[:200]}")
                return jsonify({'error': f'Invalid response from Dropbox: {str(json_err)}'}), 500
            entries = result.get('entries', [])
            
            # Collect folders and audio files
            all_folders = []
            for entry in entries:
                if entry.get('.tag') == 'folder':
                    all_folders.append({
                        'name': entry.get('name'),
                        'path': entry.get('path_display'),
                        'path_lower': entry.get('path_lower'),
                        'type': 'folder'
                    })
                elif entry.get('.tag') == 'file':
                    name = entry.get('name', '').lower()
                    if name.endswith('.mp3') or name.endswith('.wav'):
                        all_files.append({
                            'name': entry.get('name'),
                            'path': entry.get('path_display'),
                            'path_lower': entry.get('path_lower'),
                            'size': entry.get('size', 0),
                            'size_mb': round(entry.get('size', 0) / (1024 * 1024), 2),
                            'id': entry.get('id'),
                            'type': 'file'
                        })
            
            has_more = result.get('has_more', False)
            cursor = result.get('cursor')
        
        # Sort folders and files
        sorted_folders = sorted(all_folders, key=lambda x: x['name'].lower())
        sorted_files = sorted(all_files, key=lambda x: x['name'].lower())
        
        print(f"📦 Found {len(sorted_folders)} folders, {len(sorted_files)} audio files")
        
        return jsonify({
            'success': True,
            'folder': folder_path or '/',
            'total_folders': len(sorted_folders),
            'total_files': len(sorted_files),
            'folders': sorted_folders,
            'files': sorted_files
        })
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Dropbox network error: {str(e)}")
        return jsonify({'error': f'Network error: {str(e)}'}), 500
    except Exception as e:
        import traceback
        print(f"❌ Dropbox list error: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': f'Error listing Dropbox folder: {str(e)}'}), 500


@app.route('/dropbox/scan_all', methods=['GET'])
def dropbox_scan_all_files():
    """
    Recursively scan entire Dropbox (or a folder) for all MP3/WAV files.
    Uses Server-Sent Events to stream results in real-time.
    
    Query params:
    - folder_path: Optional starting folder (empty for entire Dropbox)
    
    Returns SSE stream with files as they are found.
    """
    from flask import Response, stream_with_context
    
    load_dotenv(override=True)
    
    dropbox_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    folder_path = request.args.get('folder_path', '').strip()
    
    # Normalize path
    if folder_path and not folder_path.startswith('/'):
        folder_path = '/' + folder_path
    if folder_path == '/':
        folder_path = ''
    
    def generate():
        try:
            print(f"📦 Dropbox SCAN ALL (streaming) - Folder: '{folder_path or '(root)'}'")
            
            if not dropbox_token:
                yield f"data: {json.dumps({'error': 'Dropbox not configured'})}\n\n"
                return
            
            headers = {
                'Authorization': f'Bearer {dropbox_token}',
                'Content-Type': 'application/json'
            }
            
            if dropbox_team_member_id:
                headers['Dropbox-API-Select-User'] = dropbox_team_member_id
            
            # AUTO-DETECT root namespace for team accounts
            namespace_id = ''
            if dropbox_team_member_id:
                try:
                    account_headers = {
                        'Authorization': f'Bearer {dropbox_token}',
                        'Dropbox-API-Select-User': dropbox_team_member_id
                    }
                    account_response = requests.post(
                        'https://api.dropboxapi.com/2/users/get_current_account',
                        headers=account_headers
                    )
                    if account_response.status_code == 200:
                        account_data = account_response.json()
                        root_info = account_data.get('root_info', {})
                        namespace_id = root_info.get('root_namespace_id', '')
                        if namespace_id:
                            print(f"📦 Scan: Using root namespace: {namespace_id}")
                            yield f"data: {json.dumps({'status': 'info', 'message': f'Using team namespace: {namespace_id[:8]}...'})}\n\n"
                except Exception as e:
                    print(f"⚠️ Namespace detection error: {e}")
            
            if namespace_id:
                headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})
            
            yield f"data: {json.dumps({'status': 'scanning', 'message': 'Starting scan...'})}\n\n"
            
            file_count = 0
            total_size = 0
            has_more = True
            cursor = None
            
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
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('error_summary', response.text or 'Unknown error')
                    print(f"❌ Dropbox scan error: {error_msg}")
                    yield f"data: {json.dumps({'error': error_msg})}\n\n"
                    return
                
                result = response.json()
                entries = result.get('entries', [])
                
                # Stream each audio file as it's found
                for entry in entries:
                    if entry.get('.tag') == 'file':
                        name = entry.get('name', '').lower()
                        if name.endswith('.mp3') or name.endswith('.wav'):
                            file_count += 1
                            size_mb = round(entry.get('size', 0) / (1024 * 1024), 2)
                            total_size += size_mb
                            
                            file_data = {
                                'type': 'file',
                                'index': file_count - 1,
                                'name': entry.get('name'),
                                'path': entry.get('path_display'),
                                'path_lower': entry.get('path_lower'),
                                'size': entry.get('size', 0),
                                'size_mb': size_mb,
                                'id': entry.get('id'),
                                'folder': os.path.dirname(entry.get('path_display', ''))
                            }
                            
                            print(f"📦 Found: {entry.get('name')} ({size_mb} MB)")
                            yield f"data: {json.dumps(file_data)}\n\n"
                
                has_more = result.get('has_more', False)
                cursor = result.get('cursor')
                
                # Send progress update
                if has_more:
                    yield f"data: {json.dumps({'status': 'progress', 'count': file_count, 'size_mb': round(total_size, 2)})}\n\n"
            
            # Send completion message
            print(f"📦 SCAN COMPLETE: {file_count} files ({total_size:.1f} MB)")
            yield f"data: {json.dumps({'status': 'complete', 'total_files': file_count, 'total_size_mb': round(total_size, 2)})}\n\n"
            
        except Exception as e:
            import traceback
            print(f"❌ Scan error: {str(e)}")
            print(traceback.format_exc())
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )


# =============================================================================
# BULK IMPORT - Persistent background processing
# =============================================================================

@app.route('/dropbox/bulk_import/start', methods=['POST'])
def start_bulk_import():
    """
    Start a bulk import from Dropbox. Scans recursively and processes each track.
    Runs in background - continues even if browser closes.
    """
    global bulk_import_state
    
    with bulk_import_lock:
        if bulk_import_state['active']:
            return jsonify({
                'error': 'A bulk import is already running',
                'status': bulk_import_state['current_status']
            }), 400
    
    load_dotenv(override=True)
    
    dropbox_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    if not dropbox_token:
        return jsonify({'error': 'Dropbox not configured'}), 400
    
    data = request.json or {}
    folder_path = data.get('folder_path', '').strip()
    
    # Normalize path
    if folder_path and not folder_path.startswith('/'):
        folder_path = '/' + folder_path
    if folder_path == '/':
        folder_path = ''
    
    # Reset state
    with bulk_import_lock:
        bulk_import_state = {
            'active': True,
            'stop_requested': False,
            'folder_path': folder_path,
            'namespace_id': '',
            'started_at': time.time(),
            'total_found': 0,
            'total_scanned': 0,
            'downloaded': 0,
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'current_file': '',
            'current_status': 'starting',
            'files_queue': [],
            'completed_files': [],
            'failed_files': [],
            'skipped_files': [],
            'error': None,
            'last_update': time.time()
        }
    
    # Start background thread
    thread = threading.Thread(
        target=bulk_import_background_thread,
        args=(dropbox_token, dropbox_team_member_id, folder_path),
        daemon=True
    )
    thread.start()
    
    print(f"🚀 BULK IMPORT STARTED for folder: '{folder_path or '(root)'}'")
    
    return jsonify({
        'success': True,
        'message': f'Bulk import started for {folder_path or "entire Dropbox"}',
        'status': 'starting'
    })


@app.route('/dropbox/bulk_import/status')
def get_bulk_import_status():
    """Get current status of bulk import. Works even after browser reconnects."""
    with bulk_import_lock:
        # Calculate duration
        duration = None
        if bulk_import_state['started_at']:
            duration = int(time.time() - bulk_import_state['started_at'])
        
        return jsonify({
            'active': bulk_import_state['active'],
            'status': bulk_import_state['current_status'],
            'folder_path': bulk_import_state['folder_path'],
            'total_found': bulk_import_state['total_found'],
            'downloaded': bulk_import_state['downloaded'],
            'processed': bulk_import_state['processed'],
            'failed': bulk_import_state['failed'],
            'skipped': bulk_import_state['skipped'],
            'current_file': bulk_import_state['current_file'],
            'queue_size': len(bulk_import_state['files_queue']),
            'completed_count': len(bulk_import_state['completed_files']),
            'failed_files': bulk_import_state['failed_files'][-10:],  # Last 10 failures
            'skipped_files': bulk_import_state['skipped_files'][-10:],  # Last 10 skipped
            'error': bulk_import_state['error'],
            'duration_seconds': duration,
            'last_update': bulk_import_state['last_update']
        })


@app.route('/dropbox/bulk_import/stop', methods=['POST'])
def stop_bulk_import():
    """Request to stop the bulk import."""
    global bulk_import_state
    
    with bulk_import_lock:
        if not bulk_import_state['active']:
            return jsonify({'message': 'No bulk import is running'}), 200
        
        bulk_import_state['stop_requested'] = True
        bulk_import_state['current_status'] = 'stopping'
        bulk_import_state['last_update'] = time.time()
    
    print("⏹️ BULK IMPORT STOP REQUESTED")
    
    return jsonify({
        'success': True,
        'message': 'Stop requested. Import will stop after current file.'
    })


def bulk_import_background_thread(dropbox_token, dropbox_team_member_id, folder_path):
    """
    Background thread that downloads files from Dropbox and uses the CLASSIC workflow.
    
    Dropbox download = Just like "Select Files" in the UI
    Then uses the exact same processing as normal uploads.
    
    After each track completes successfully, delete from Dropbox.
    """
    global bulk_import_state
    
    try:
        # Setup headers
        headers = {
            'Authorization': f'Bearer {dropbox_token}',
            'Content-Type': 'application/json'
        }
        
        if dropbox_team_member_id:
            headers['Dropbox-API-Select-User'] = dropbox_team_member_id
        
        # Auto-detect namespace
        namespace_id = ''
        if dropbox_team_member_id:
            try:
                account_headers = {
                    'Authorization': f'Bearer {dropbox_token}',
                    'Dropbox-API-Select-User': dropbox_team_member_id
                }
                account_response = requests.post(
                    'https://api.dropboxapi.com/2/users/get_current_account',
                    headers=account_headers
                )
                if account_response.status_code == 200:
                    account_data = account_response.json()
                    root_info = account_data.get('root_info', {})
                    namespace_id = root_info.get('root_namespace_id', '')
                    if namespace_id:
                        print(f"📦 Bulk Import: Using namespace {namespace_id}")
                        with bulk_import_lock:
                            bulk_import_state['namespace_id'] = namespace_id
            except Exception as e:
                print(f"⚠️ Namespace detection error: {e}")
        
        if namespace_id:
            headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})
        
        # PHASE 1: Scan for all files
        with bulk_import_lock:
            bulk_import_state['current_status'] = 'scanning'
            bulk_import_state['last_update'] = time.time()
        
        print(f"📦 Scanning '{folder_path or '(root)'}' recursively...")
        
        all_files = []
        has_more = True
        cursor = None
        
        while has_more:
            # Check for stop request
            with bulk_import_lock:
                if bulk_import_state['stop_requested']:
                    bulk_import_state['current_status'] = 'stopped'
                    bulk_import_state['active'] = False
                    bulk_import_state['last_update'] = time.time()
                    print("⏹️ Bulk import stopped during scan")
                    return
            
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
                error_msg = response.json().get('error_summary', 'Unknown error') if response.text else 'Unknown error'
                with bulk_import_lock:
                    bulk_import_state['error'] = error_msg
                    bulk_import_state['current_status'] = 'error'
                    bulk_import_state['active'] = False
                    bulk_import_state['last_update'] = time.time()
                print(f"❌ Scan error: {error_msg}")
                return
            
            result = response.json()
            
            for entry in result.get('entries', []):
                if entry.get('.tag') == 'file':
                    name = entry.get('name', '').lower()
                    if name.endswith('.mp3') or name.endswith('.wav'):
                        all_files.append({
                            'name': entry.get('name'),
                            'path': entry.get('path_display'),
                            'size': entry.get('size', 0),
                            'id': entry.get('id')
                        })
                        print(f"📦 Found: {entry.get('name')}")
            
            with bulk_import_lock:
                bulk_import_state['total_scanned'] += len(result.get('entries', []))
                bulk_import_state['total_found'] = len(all_files)
                bulk_import_state['files_queue'] = all_files.copy()
                bulk_import_state['last_update'] = time.time()
            
            has_more = result.get('has_more', False)
            cursor = result.get('cursor')
        
        print(f"📦 Scan complete: {len(all_files)} audio files found")
        
        if len(all_files) == 0:
            with bulk_import_lock:
                bulk_import_state['current_status'] = 'complete'
                bulk_import_state['active'] = False
                bulk_import_state['last_update'] = time.time()
            print("📦 No files to import")
            return
        
        # PHASE 2: Download all files and queue them (like selecting files + clicking upload)
        with bulk_import_lock:
            bulk_import_state['current_status'] = 'downloading'
            bulk_import_state['last_update'] = time.time()
        
        # Use the GLOBAL session for bulk import (so it shows in All Tracks)
        bulk_session_id = 'global'
        
        # Map: safe_filename -> dropbox_path (for deletion after success)
        dropbox_paths = {}
        dropbox_paths_lock = Lock()
        
        # Download concurrency - match number of workers for speed
        download_workers = NUM_WORKERS
        print(f"🚀 Downloading {len(all_files)} files with {download_workers} parallel downloads")
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # Track skipped files count
        skipped_count = [0]  # Use list for mutability in nested function
        
        def download_file(file_info, index):
            """Download a single file from Dropbox to upload folder."""
            dropbox_path = file_info['path']
            file_name = file_info['name']
            
            # Check for stop
            with bulk_import_lock:
                if bulk_import_state['stop_requested']:
                    return {'status': 'stopped', 'name': file_name}
            
            # =====================================================
            # TITLE FILTERING - Skip tracks with banned keywords
            # =====================================================
            title_result = process_track_title_for_import(file_name)
            
            if title_result['skip']:
                print(f"⏭️  [{index+1}/{len(all_files)}] SKIPPING: {file_name}")
                print(f"   Reason: {title_result['skip_reason']}")
                
                # Delete from Dropbox since it's a banned track
                if delete_from_dropbox_if_skipped(dropbox_path, dropbox_token, dropbox_team_member_id, namespace_id):
                    print(f"   🗑️  Deleted from Dropbox")
                else:
                    print(f"   ⚠️  Could not delete from Dropbox")
                
                skipped_count[0] += 1
                with bulk_import_lock:
                    bulk_import_state['skipped'] += 1
                    bulk_import_state['skipped_files'].append({
                        'name': file_name,
                        'reason': title_result['skip_reason']
                    })
                    bulk_import_state['last_update'] = time.time()
                
                return {'status': 'skipped', 'name': file_name, 'reason': title_result['skip_reason']}
            
            try:
                # Use cleaned title for the filename
                cleaned_title = title_result['cleaned_title']
                base_name = os.path.splitext(file_name)[0]
                extension = os.path.splitext(file_name)[1]
                
                # Sanitize the cleaned filename
                safe_filename = re.sub(r'[^\w\s\-\.]', '', cleaned_title).strip() or f'track_{index}'
                safe_filename = safe_filename + extension
                local_path = os.path.join(UPLOAD_FOLDER, safe_filename)
                
                # Add to queue tracker IMMEDIATELY so it shows in "All Tracks"
                with queue_items_lock:
                    queue_items[safe_filename] = {
                        'status': 'waiting',
                        'worker': None,
                        'progress': 0,
                        'session_id': bulk_session_id,
                        'step': '⬇️ Downloading from Dropbox...',
                        'added_at': time.time(),
                        'processing_started_at': None
                    }
                
                print(f"⬇️  [{index+1}/{len(all_files)}] Downloading: {file_name}")
                if cleaned_title != base_name:
                    print(f"   → Cleaned: {cleaned_title}")
                
                download_headers = {
                    'Authorization': f'Bearer {dropbox_token}',
                    'Dropbox-API-Arg': json.dumps({'path': dropbox_path})
                }
                if dropbox_team_member_id:
                    download_headers['Dropbox-API-Select-User'] = dropbox_team_member_id
                if namespace_id:
                    download_headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})
                
                download_response = requests.post(
                    'https://content.dropboxapi.com/2/files/download',
                    headers=download_headers,
                    stream=True
                )
                
                if download_response.status_code != 200:
                    raise Exception(f'Download failed: HTTP {download_response.status_code}')
                
                # Save file locally
                with open(local_path, 'wb') as f:
                    for chunk in download_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                with bulk_import_lock:
                    bulk_import_state['downloaded'] += 1
                    bulk_import_state['last_update'] = time.time()
                
                # Update queue tracker - download complete, waiting for processing
                with queue_items_lock:
                    if safe_filename in queue_items:
                        queue_items[safe_filename]['step'] = '✅ Downloaded, waiting...'
                
                # Store mapping for Dropbox deletion
                with dropbox_paths_lock:
                    dropbox_paths[safe_filename] = dropbox_path
                
                print(f"✅ Downloaded: {file_name} -> {safe_filename}")
                return {'status': 'ok', 'name': file_name, 'safe_filename': safe_filename, 'local_path': local_path}
                
            except Exception as e:
                print(f"❌ Download failed: {file_name} - {str(e)}")
                
                # Update queue tracker - failed
                with queue_items_lock:
                    if safe_filename in queue_items:
                        queue_items[safe_filename]['status'] = 'failed'
                        queue_items[safe_filename]['step'] = f'❌ Download failed: {str(e)[:50]}'
                
                with bulk_import_lock:
                    bulk_import_state['failed'] += 1
                    bulk_import_state['failed_files'].append({
                        'name': file_name,
                        'error': str(e)
                    })
                    bulk_import_state['last_update'] = time.time()
                return {'status': 'failed', 'name': file_name, 'error': str(e)}
        
        # Download all files in parallel
        downloaded_files = []
        with ThreadPoolExecutor(max_workers=download_workers) as executor:
            futures = {
                executor.submit(download_file, file_info, i): file_info
                for i, file_info in enumerate(all_files)
            }
            
            for future in as_completed(futures):
                result = future.result()
                if result['status'] == 'stopped':
                    print("⏹️ Bulk import stopped during downloads")
                    with bulk_import_lock:
                        bulk_import_state['current_status'] = 'stopped'
                        bulk_import_state['active'] = False
                    return
                elif result['status'] == 'ok':
                    downloaded_files.append(result)
        
        print(f"📦 Downloaded {len(downloaded_files)} files. Now queueing for processing...")
        
        # PHASE 3: Queue all files for processing (CLASSIC WORKFLOW)
        with bulk_import_lock:
            bulk_import_state['current_status'] = 'processing'
            bulk_import_state['last_update'] = time.time()
        
        for file_result in downloaded_files:
            safe_filename = file_result['safe_filename']
            
            # Update queue tracker status (already added during download)
            with queue_items_lock:
                if safe_filename in queue_items:
                    queue_items[safe_filename]['step'] = 'En attente...'
                else:
                    # Add if not present (fallback)
                    queue_items[safe_filename] = {
                        'status': 'waiting',
                        'worker': None,
                        'progress': 0,
                        'session_id': bulk_session_id,
                        'step': 'En attente...',
                        'added_at': time.time(),
                        'processing_started_at': None
                    }
            
            # Queue item for worker processing
            track_queue.put({
                'filename': safe_filename,
                'session_id': bulk_session_id,
                'is_retry': False
            })
            
            print(f"📋 Queued: {safe_filename}")
        
        print(f"📦 All {len(downloaded_files)} files queued! Workers will process them.")
        print(f"🚀 Using {NUM_WORKERS} workers for parallel processing")
        
        # PHASE 4: Monitor and delete from Dropbox when tracks complete
        with bulk_import_lock:
            bulk_import_state['current_file'] = f'Processing {len(downloaded_files)} tracks...'
        
        completed_tracks = set()
        
        while True:
            # Check for stop
            with bulk_import_lock:
                if bulk_import_state['stop_requested']:
                    bulk_import_state['current_status'] = 'stopped'
                    bulk_import_state['active'] = False
                    print("⏹️ Bulk import stopped")
                    break
            
            time.sleep(3)  # Check every 3 seconds
            
            # Check queue items for completed tracks
            with queue_items_lock:
                for filename, info in list(queue_items.items()):
                    if filename in dropbox_paths and filename not in completed_tracks:
                        # Check if track is done (not in queue anymore or status is done/failed)
                        if info.get('status') in ('done', 'failed') or filename not in queue_items:
                            completed_tracks.add(filename)
                            
                            if info.get('status') == 'done' or info.get('status') not in ('waiting', 'processing', 'failed'):
                                # SUCCESS - Delete from Dropbox
                                dropbox_path = dropbox_paths[filename]
                                with bulk_import_lock:
                                    bulk_import_state['processed'] += 1
                                    bulk_import_state['completed_files'].append(filename)
                                    bulk_import_state['last_update'] = time.time()
                                
                                try:
                                    print(f"🗑️  Deleting from Dropbox: {filename}")
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
                                    
                                    if delete_response.status_code == 200:
                                        print(f"✅ Deleted from Dropbox: {filename}")
                                    else:
                                        print(f"⚠️  Could not delete: {delete_response.text[:100]}")
                                except Exception as e:
                                    print(f"⚠️  Error deleting from Dropbox: {e}")
                            
                            elif info.get('status') == 'failed':
                                # FAILED - Keep in Dropbox
                                with bulk_import_lock:
                                    if not any(f.get('name') == filename for f in bulk_import_state['failed_files']):
                                        bulk_import_state['failed_files'].append({
                                            'name': filename,
                                            'error': info.get('step', 'Processing failed')
                                        })
                                    bulk_import_state['last_update'] = time.time()
                                print(f"❌ Failed (kept in Dropbox): {filename}")
            
            # Also check for tracks no longer in queue (processed successfully)
            with dropbox_paths_lock:
                for filename, dropbox_path in list(dropbox_paths.items()):
                    if filename not in completed_tracks:
                        with queue_items_lock:
                            if filename not in queue_items:
                                # Track finished and was removed from queue = success
                                completed_tracks.add(filename)
                                with bulk_import_lock:
                                    bulk_import_state['processed'] += 1
                                    bulk_import_state['completed_files'].append(filename)
                                    bulk_import_state['last_update'] = time.time()
                                
                                try:
                                    print(f"🗑️  Deleting from Dropbox: {filename}")
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
                                    
                                    if delete_response.status_code == 200:
                                        print(f"✅ Deleted from Dropbox: {filename}")
                                except Exception as e:
                                    print(f"⚠️  Error deleting from Dropbox: {e}")
            
            # Update status display
            with bulk_import_lock:
                in_progress = len(downloaded_files) - len(completed_tracks)
                bulk_import_state['current_file'] = f'⚙️ {in_progress} processing, ✅ {bulk_import_state["processed"]} done'
            
            # Check if all done
            if len(completed_tracks) >= len(downloaded_files):
                print(f"🎉 All {len(downloaded_files)} tracks processed!")
                break
        
        # Complete
        with bulk_import_lock:
            bulk_import_state['current_status'] = 'complete'
            bulk_import_state['active'] = False
            bulk_import_state['current_file'] = ''
            bulk_import_state['last_update'] = time.time()
        
        print(f"\n{'='*60}")
        print(f"🎉 BULK IMPORT COMPLETE!")
        print(f"   Total found: {bulk_import_state['total_found']}")
        print(f"   Skipped (banned keywords): {bulk_import_state['skipped']}")
        print(f"   Downloaded: {len(downloaded_files)}")
        print(f"   Processed: {bulk_import_state['processed']}")
        print(f"   Failed: {bulk_import_state['failed']}")
        print(f"{'='*60}")
        
    except Exception as e:
        import traceback
        print(f"❌ Bulk import error: {str(e)}")
        print(traceback.format_exc())
        with bulk_import_lock:
            bulk_import_state['error'] = str(e)
            bulk_import_state['current_status'] = 'error'
            bulk_import_state['active'] = False
            bulk_import_state['last_update'] = time.time()


@app.route('/dropbox/import', methods=['POST'])
def dropbox_import_files():
    """
    Import files from Dropbox and process them with the classic workflow.
    Downloads files in background and enqueues them for processing.
    
    JSON body:
    - folder_path: Dropbox folder path to import from
    - files: Optional list of specific file paths to import (if empty, imports all)
    
    Returns import_id to track progress.
    """
    # Reload .env in case token was added after startup
    load_dotenv(override=True)
    
    # Re-read token from environment
    dropbox_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    if not dropbox_token:
        return jsonify({'error': 'Dropbox not configured. Set DROPBOX_ACCESS_TOKEN in .env'}), 400
    
    session_id = get_session_id()
    data = request.json or {}
    folder_path = data.get('folder_path', '').strip()
    specific_files = data.get('files', [])  # Optional: specific files to import
    
    # Normalize path
    if folder_path and not folder_path.startswith('/'):
        folder_path = '/' + folder_path
    if folder_path == '/':
        folder_path = ''
    
    # Generate import ID
    import_id = f"dropbox_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    # First, get the list of files to import
    try:
        headers = {
            'Authorization': f'Bearer {dropbox_token}',
            'Content-Type': 'application/json'
        }
        
        # Add team member header for Dropbox Business team tokens
        if dropbox_team_member_id:
            headers['Dropbox-API-Select-User'] = dropbox_team_member_id
        
        # AUTO-DETECT root namespace for team accounts
        # This is crucial for accessing content inside team folders
        if dropbox_team_member_id:
            try:
                # Note: get_current_account requires no JSON body
                account_headers = {
                    'Authorization': f'Bearer {dropbox_token}',
                    'Dropbox-API-Select-User': dropbox_team_member_id
                }
                account_response = requests.post(
                    'https://api.dropboxapi.com/2/users/get_current_account',
                    headers=account_headers
                )
                if account_response.status_code == 200:
                    account_data = account_response.json()
                    root_info = account_data.get('root_info', {})
                    root_ns = root_info.get('root_namespace_id')
                    if root_ns:
                        headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': root_ns})
                        print(f"📦 Import: Using root namespace: {root_ns}")
            except Exception as e:
                print(f"⚠️ Could not auto-detect namespace for import: {e}")
        
        files_to_import = []
        
        if specific_files:
            # Use specific files provided
            for file_path in specific_files:
                name = os.path.basename(file_path)
                if name.lower().endswith('.mp3') or name.lower().endswith('.wav'):
                    files_to_import.append({
                        'name': name,
                        'path': file_path,
                        'path_lower': file_path.lower()
                    })
        else:
            # List all files in folder
            has_more = True
            cursor = None
            
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
                            'include_deleted': False
                        }
                    )
                
                if response.status_code != 200:
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('error_summary', 'Unknown error')
                    return jsonify({'error': f'Dropbox API error: {error_msg}'}), response.status_code
                
                result = response.json()
                
                for entry in result.get('entries', []):
                    if entry.get('.tag') == 'file':
                        name = entry.get('name', '').lower()
                        if name.endswith('.mp3') or name.endswith('.wav'):
                            files_to_import.append({
                                'name': entry.get('name'),
                                'path': entry.get('path_display'),
                                'path_lower': entry.get('path_lower'),
                                'size': entry.get('size', 0)
                            })
                
                has_more = result.get('has_more', False)
                cursor = result.get('cursor')
        
        if not files_to_import:
            return jsonify({'error': 'No audio files found in the specified folder'}), 404
        
        # Initialize import tracking
        with dropbox_imports_lock:
            dropbox_imports[import_id] = {
                'status': 'downloading',
                'total': len(files_to_import),
                'downloaded': 0,
                'queued': 0,
                'processed': 0,
                'failed': 0,
                'files': {f['name']: {'status': 'pending', 'path': f['path']} for f in files_to_import},
                'errors': [],
                'started_at': time.time(),
                'session_id': session_id,
                'folder_path': folder_path
            }
        
        # Get root namespace for downloads (needed for team folders)
        root_namespace_id = ''
        if dropbox_team_member_id:
            try:
                # Note: get_current_account requires no JSON body
                account_headers = {
                    'Authorization': f'Bearer {dropbox_token}',
                    'Dropbox-API-Select-User': dropbox_team_member_id
                }
                account_response = requests.post(
                    'https://api.dropboxapi.com/2/users/get_current_account',
                    headers=account_headers
                )
                if account_response.status_code == 200:
                    account_data = account_response.json()
                    root_info = account_data.get('root_info', {})
                    root_namespace_id = root_info.get('root_namespace_id', '')
            except:
                pass
        
        # Start background thread to download and process files
        thread = threading.Thread(
            target=dropbox_download_and_process_thread,
            args=(import_id, files_to_import, session_id, dropbox_token, dropbox_team_member_id, root_namespace_id)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'import_id': import_id,
            'total_files': len(files_to_import),
            'message': f'Started importing {len(files_to_import)} files from Dropbox'
        })
        
    except Exception as e:
        return jsonify({'error': f'Error starting import: {str(e)}'}), 500


def dropbox_download_and_process_thread(import_id, files_to_import, session_id, dropbox_token, dropbox_team_member_id='', root_namespace_id=''):
    """
    Background thread to download files from Dropbox and enqueue for processing.
    """
    headers = {
        'Authorization': f'Bearer {dropbox_token}',
    }
    
    # Add team member header for Dropbox Business team tokens
    if dropbox_team_member_id:
        headers['Dropbox-API-Select-User'] = dropbox_team_member_id
    
    print(f"📦 Download thread started with namespace: {root_namespace_id}")
    
    # Create session-specific upload folder
    session_upload_folder = os.path.join(UPLOAD_FOLDER, session_id)
    os.makedirs(session_upload_folder, exist_ok=True)
    
    downloaded_files = []
    
    for file_info in files_to_import:
        file_path = file_info['path']
        file_name = file_info['name']
        
        try:
            # Update status
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['files'][file_name]['status'] = 'downloading'
            
            # Download file from Dropbox
            download_headers = {
                'Authorization': f'Bearer {dropbox_token}',
                'Dropbox-API-Arg': json.dumps({'path': file_path})
            }
            if dropbox_team_member_id:
                download_headers['Dropbox-API-Select-User'] = dropbox_team_member_id
            
            # Add namespace header if available (passed from the import thread)
            if root_namespace_id:
                download_headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': root_namespace_id})
            
            download_response = requests.post(
                'https://content.dropboxapi.com/2/files/download',
                headers=download_headers,
                stream=True
            )
            
            if download_response.status_code != 200:
                raise Exception(f'Download failed: {download_response.status_code}')
            
            # Save file locally
            safe_filename = re.sub(r'[^\w\s\-\.]', '', file_name)
            safe_filename = safe_filename.strip() or f'track_{len(downloaded_files)}.mp3'
            local_path = os.path.join(session_upload_folder, safe_filename)
            
            with open(local_path, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            downloaded_files.append({
                'name': safe_filename,
                'original_name': file_name,
                'path': local_path
            })
            
            # Update downloaded count
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['downloaded'] += 1
                    dropbox_imports[import_id]['files'][file_name]['status'] = 'downloaded'
                    dropbox_imports[import_id]['files'][file_name]['local_path'] = local_path
            
            print(f"📥 Downloaded from Dropbox: {file_name}")
            
        except Exception as e:
            print(f"❌ Failed to download {file_name}: {str(e)}")
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['failed'] += 1
                    dropbox_imports[import_id]['files'][file_name]['status'] = 'failed'
                    dropbox_imports[import_id]['files'][file_name]['error'] = str(e)
                    dropbox_imports[import_id]['errors'].append({
                        'file': file_name,
                        'error': str(e)
                    })
    
    # Update status to queueing
    with dropbox_imports_lock:
        if import_id in dropbox_imports:
            dropbox_imports[import_id]['status'] = 'queueing'
    
    # Now enqueue all downloaded files for processing
    for file_info in downloaded_files:
        try:
            local_path = file_info['path']
            filename = file_info['name']
            
            # Check if already processed
            is_processed, _ = is_track_already_processed(filename)
            if is_processed:
                print(f"⏭️ Already processed: {filename}")
                with dropbox_imports_lock:
                    if import_id in dropbox_imports:
                        dropbox_imports[import_id]['files'][file_info['original_name']]['status'] = 'skipped'
                continue
            
            # Add to processing queue
            track_queue.put({
                'filepath': local_path,
                'filename': filename,
                'session_id': session_id,
                'priority': 0
            })
            
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['queued'] += 1
                    dropbox_imports[import_id]['files'][file_info['original_name']]['status'] = 'queued'
            
            print(f"📋 Queued for processing: {filename}")
            
        except Exception as e:
            print(f"❌ Failed to queue {file_info['name']}: {str(e)}")
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['errors'].append({
                        'file': file_info['name'],
                        'error': f'Queue error: {str(e)}'
                    })
    
    # Mark as complete
    with dropbox_imports_lock:
        if import_id in dropbox_imports:
            dropbox_imports[import_id]['status'] = 'processing'
            dropbox_imports[import_id]['completed_at'] = time.time()
    
    print(f"✅ Dropbox import {import_id} complete: {len(downloaded_files)} files queued for processing")


@app.route('/dropbox/status/<import_id>')
def dropbox_import_status(import_id):
    """Get status of a Dropbox import operation."""
    with dropbox_imports_lock:
        if import_id not in dropbox_imports:
            return jsonify({'error': 'Import not found'}), 404
        
        status = dropbox_imports[import_id].copy()
    
    return jsonify(status)


@app.route('/dropbox/status')
def dropbox_all_imports_status():
    """Get status of all Dropbox imports for current session."""
    session_id = get_session_id()
    
    with dropbox_imports_lock:
        session_imports = {
            k: v.copy() for k, v in dropbox_imports.items()
            if v.get('session_id') == session_id
        }
    
    # Re-read token to check if configured
    dropbox_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
    
    return jsonify({
        'imports': session_imports,
        'dropbox_configured': bool(dropbox_token)
    })


@app.route('/dropbox/configured')
def dropbox_configured():
    """Check if Dropbox is configured."""
    # Reload .env in case token was added after startup
    load_dotenv(override=True)
    
    # Re-read token from environment
    dropbox_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
    
    return jsonify({
        'configured': bool(dropbox_token),
        'message': 'Dropbox is configured' if dropbox_token else 'Set DROPBOX_ACCESS_TOKEN in .env'
    })


@app.route('/dropbox/namespaces')
def dropbox_get_namespaces():
    """
    Get available namespaces for Dropbox team accounts.
    This helps find team folders that require a specific namespace_id.
    """
    load_dotenv(override=True)
    dropbox_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    if not dropbox_token:
        return jsonify({'error': 'Dropbox not configured'}), 400
    
    try:
        # Get current account info to find namespace
        headers = {
            'Authorization': f'Bearer {dropbox_token}',
            'Content-Type': 'application/json'
        }
        
        # Add team member header for Dropbox Business team tokens
        if dropbox_team_member_id:
            headers['Dropbox-API-Select-User'] = dropbox_team_member_id
            print(f"📦 Using team member ID: {dropbox_team_member_id[:20]}...")
        
        # First get current account (no JSON body needed for this endpoint)
        account_headers = {
            'Authorization': f'Bearer {dropbox_token}'
        }
        if dropbox_team_member_id:
            account_headers['Dropbox-API-Select-User'] = dropbox_team_member_id
        
        account_response = requests.post(
            'https://api.dropboxapi.com/2/users/get_current_account',
            headers=account_headers
        )
        
        print(f"📦 Account response status: {account_response.status_code}")
        
        namespaces = []
        account_info = {}
        
        if account_response.status_code == 200:
            account_data = account_response.json()
            account_info = {
                'name': account_data.get('name', {}).get('display_name', 'Unknown'),
                'email': account_data.get('email', ''),
                'account_type': account_data.get('account_type', {}).get('.tag', 'unknown')
            }
            
            # Get root namespace info
            root_info = account_data.get('root_info', {})
            print(f"📦 Root info from Dropbox: {root_info}")
            
            if root_info:
                # Home namespace (personal files)
                home_ns = root_info.get('home_namespace_id')
                # Root namespace (team root for team accounts)
                root_ns = root_info.get('root_namespace_id')
                
                print(f"📦 Home namespace: {home_ns}, Root namespace: {root_ns}")
                
                # For team accounts, the root_namespace_id is what you need to access team folders
                # Always add the root namespace first (it's needed for team folder contents)
                if root_ns:
                    namespaces.append({
                        'id': root_ns,
                        'name': 'Team Root' if root_ns != home_ns else 'Root',
                        'type': 'team_root' if root_ns != home_ns else 'root'
                    })
                
                # Add home namespace separately if different from root
                if home_ns and home_ns != root_ns:
                    namespaces.append({
                        'id': home_ns,
                        'name': 'Home (Personal Files)',
                        'type': 'home'
                    })
            
            print(f"📦 Account: {account_info}")
            print(f"📦 Namespaces found: {namespaces}")
        else:
            print(f"❌ Account response error: {account_response.text[:500] if account_response.text else 'empty'}")
        
        # Try to list shared folders (team folders appear here)
        shared_response = requests.post(
            'https://api.dropboxapi.com/2/sharing/list_folders',
            headers=headers,
            json={'limit': 100}
        )
        
        shared_folders = []
        if shared_response.status_code == 200:
            shared_data = shared_response.json()
            for entry in shared_data.get('entries', []):
                shared_folders.append({
                    'name': entry.get('name'),
                    'shared_folder_id': entry.get('shared_folder_id'),
                    'path_lower': entry.get('path_lower'),
                    'is_team_folder': entry.get('is_team_folder', False),
                    'is_inside_team_folder': entry.get('is_inside_team_folder', False)
                })
            print(f"📦 Shared folders: {len(shared_folders)}")
        
        return jsonify({
            'success': True,
            'account': account_info,
            'namespaces': namespaces,
            'shared_folders': shared_folders
        })
        
    except Exception as e:
        import traceback
        print(f"❌ Error getting namespaces: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


@app.route('/status')
def status():
    # Get session-specific status
    session_id = request.args.get('session_id') or get_session_id()
    current_status = get_job_status(session_id)
    
    # Update queue info
    current_status['queue_size'] = track_queue.qsize()
    current_status['num_workers'] = NUM_WORKERS
    current_status['active_workers'] = sum(1 for t in worker_threads if t.is_alive())
    current_status['queue_items'] = get_queue_items_list()
    
    # Add pending downloads info and warning
    current_status['pending_downloads'] = get_pending_tracks_count()
    current_status['pending_warning'] = check_pending_tracks_warning()
    
    # Ensure failed_files is included (for frontend display)
    if 'failed_files' not in current_status:
        current_status['failed_files'] = []
    
    # Return session-specific status
    return jsonify(current_status)

@app.route('/retry_failed', methods=['POST'])
def retry_failed():
    """
    Retry processing failed files.
    Can retry all failed files or specific ones by filename.
    """
    session_id = get_session_id()
    current_status = get_job_status(session_id)
    
    data = request.json or {}
    specific_files = data.get('filenames', [])  # Empty = retry all
    
    failed_files = current_status.get('failed_files', [])
    
    if not failed_files:
        return jsonify({'message': 'Aucun fichier en échec à réessayer', 'retried': 0})
    
    retried_count = 0
    retried_files = []
    
    for failed in list(failed_files):  # Use list() to avoid modifying while iterating
        filename = failed['filename']
        
        # If specific files requested, only retry those
        if specific_files and filename not in specific_files:
            continue
        
        # Reset retry count for fresh retry
        current_status['retry_count'][filename] = 0
        
        # Remove from failed list (will be re-added if still fails)
        remove_failed_file(session_id, filename)
        
        # Add back to queue as retry
        add_to_queue_tracker(filename, session_id)
        track_queue.put({
            'filename': filename,
            'session_id': session_id,
            'is_retry': True
        })
        
        retried_count += 1
        retried_files.append(filename)
        log_message(f"🔄 [{session_id}] Réessai ajouté à la file: {filename}", session_id)
    
    return jsonify({
        'message': f'{retried_count} fichier(s) ajouté(s) à la file pour réessai',
        'retried': retried_count,
        'filenames': retried_files,
        'queue_size': track_queue.qsize()
    })

@app.route('/clear_failed', methods=['POST'])
def clear_failed():
    """
    Clear the failed files list without retrying.
    Use this to acknowledge failures and clear them from the UI.
    """
    session_id = get_session_id()
    current_status = get_job_status(session_id)
    
    data = request.json or {}
    specific_files = data.get('filenames', [])  # Empty = clear all
    
    failed_files = current_status.get('failed_files', [])
    
    if not failed_files:
        return jsonify({'message': 'Aucun fichier en échec à effacer', 'cleared': 0})
    
    cleared_count = 0
    
    if specific_files:
        # Clear only specific files
        for filename in specific_files:
            remove_failed_file(session_id, filename)
            remove_from_queue_tracker(filename)
            cleared_count += 1
    else:
        # Clear all failed files
        cleared_count = len(failed_files)
        current_status['failed_files'] = []
        # Also remove from queue tracker
        for failed in failed_files:
            remove_from_queue_tracker(failed['filename'])
    
    log_message(f"🗑️ [{session_id}] {cleared_count} fichier(s) en échec effacé(s)", session_id)
    
    return jsonify({
        'message': f'{cleared_count} fichier(s) effacé(s)',
        'cleared': cleared_count
    })

@app.route('/failed_files')
def get_failed_files():
    """
    Get the list of failed files for the current session.
    """
    session_id = request.args.get('session_id') or get_session_id()
    current_status = get_job_status(session_id)
    
    failed_files = current_status.get('failed_files', [])
    
    return jsonify({
        'failed_files': failed_files,
        'count': len(failed_files),
        'session_id': session_id
    })

@app.route('/reset_stuck_items', methods=['POST'])
def reset_stuck_items():
    """
    Reset all items stuck in 'processing' state.
    Use this to recover from a crashed state where items show as processing
    but no workers are actually working on them.
    """
    session_id = get_session_id()
    reset_count = 0
    reset_files = []
    
    with queue_items_lock:
        for filename, info in queue_items.items():
            if info['status'] == 'processing':
                # Reset to waiting so it can be reprocessed
                info['status'] = 'waiting'
                info['worker'] = None
                info['progress'] = 0
                info['step'] = 'Reset - En attente...'
                info['processing_started_at'] = None
                reset_count += 1
                reset_files.append(filename)
    
    if reset_count > 0:
        log_message(f"🔄 [{session_id}] Reset {reset_count} stuck processing item(s)", session_id)
        
        # Re-queue the items so workers can pick them up again
        for filename in reset_files:
            with queue_items_lock:
                file_session_id = queue_items.get(filename, {}).get('session_id', session_id)
            track_queue.put({'filename': filename, 'session_id': file_session_id, 'is_retry': True})
    
    return jsonify({
        'message': f'Reset {reset_count} stuck item(s)',
        'reset': reset_count,
        'filenames': reset_files,
        'queue_size': track_queue.qsize()
    })

@app.route('/queue_debug')
def queue_debug():
    """
    Debug endpoint to inspect the current state of the queue and workers.
    """
    with queue_items_lock:
        items_by_status = {}
        for filename, info in queue_items.items():
            status = info['status']
            if status not in items_by_status:
                items_by_status[status] = []
            items_by_status[status].append({
                'filename': filename,
                'worker': info.get('worker'),
                'progress': info.get('progress'),
                'processing_started_at': info.get('processing_started_at'),
                'time_processing': round(time.time() - info.get('processing_started_at', time.time())) if info.get('processing_started_at') else None
            })
    
    return jsonify({
        'total_items': len(queue_items),
        'queue_size': track_queue.qsize(),
        'num_workers': NUM_WORKERS,
        'active_workers': sum(1 for t in worker_threads if t.is_alive()),
        'items_by_status': items_by_status,
        'max_processing_time_seconds': MAX_PROCESSING_TIME
    })

@app.route('/system_stats')
def system_stats():
    """Returns real-time system statistics for the UI."""
    stats = {
        'cpu': {
            'count': CPU_COUNT,
            'percent': 0,
        },
        'memory': {
            'total_gb': 0,
            'used_percent': 0,
            'available_gb': 0,
        },
        'gpu': {
            'name': 'CPU Mode',
            'memory_gb': 0,
            'memory_used_percent': 0,
            'available': False,
        },
        'processing': {
            'device': DEMUCS_DEVICE,
            'num_workers': NUM_WORKERS,
            'queue_size': track_queue.qsize(),
            'active_workers': sum(1 for t in worker_threads if t.is_alive()),
        },
        'disk': {
            'total_gb': 0,
            'used_percent': 0,
            'free_gb': 0,
        }
    }
    
    # CPU/Memory stats
    try:
        import psutil
        stats['cpu']['percent'] = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        stats['memory']['total_gb'] = round(mem.total / (1024**3), 1)
        stats['memory']['used_percent'] = mem.percent
        stats['memory']['available_gb'] = round(mem.available / (1024**3), 1)
        
        disk = psutil.disk_usage('/')
        stats['disk']['total_gb'] = round(disk.total / (1024**3), 1)
        stats['disk']['used_percent'] = disk.percent
        stats['disk']['free_gb'] = round(disk.free / (1024**3), 1)
    except:
        pass
    
    # GPU stats
    try:
        import torch
        if torch.cuda.is_available():
            stats['gpu']['available'] = True
            stats['gpu']['name'] = torch.cuda.get_device_name(0)
            props = torch.cuda.get_device_properties(0)
            stats['gpu']['memory_gb'] = round(props.total_memory / (1024**3), 1)
            
            allocated = torch.cuda.memory_allocated(0)
            stats['gpu']['memory_used_gb'] = round(allocated / (1024**3), 2)
            stats['gpu']['memory_used_percent'] = round(allocated / props.total_memory * 100, 1)
    except:
        pass
    
    return jsonify(stats)

@app.route('/download_file')
def download_file():
    """
    Robust download route using query parameter.
    Usage: /download_file?path=SubDir/File.mp3
    Automatically deletes file after successful download if API confirmed.
    """
    relative_path = request.args.get('path')
    
    print(f"📥 DOWNLOAD REQUEST")
    print(f"   Raw path param: {relative_path}")
    
    if not relative_path:
        print("   ❌ No path provided")
        abort(400)
    
    # Security: prevent directory traversal
    if '..' in relative_path:
        print("   ❌ Directory traversal attempt")
        abort(403)
    
    # URL decode the path (in case it's double-encoded)
    decoded_path = urllib.parse.unquote(relative_path)
    print(f"   Decoded path: {decoded_path}")
        
    # Construct full path
    filepath = os.path.join(PROCESSED_FOLDER, decoded_path)
    
    print(f"   Looking for: {filepath}")
    print(f"   File exists: {os.path.exists(filepath)}")
    
    # Extract track name from path (first directory component)
    track_name = decoded_path.split('/')[0] if '/' in decoded_path else None
    
    # If not found, try to find a matching file (handle encoding issues)
    if not os.path.exists(filepath):
        # Try to find file with similar name
        parts = decoded_path.split('/')
        if len(parts) >= 2:
            subdir_name = parts[0]
            file_name = parts[1]
            
            # Look for matching subdirectory
            for existing_dir in os.listdir(PROCESSED_FOLDER):
                if existing_dir.lower() == subdir_name.lower() or existing_dir == subdir_name:
                    subdir_path = os.path.join(PROCESSED_FOLDER, existing_dir)
                    track_name = existing_dir  # Update track name to actual folder name
                    if os.path.isdir(subdir_path):
                        # Look for matching file
                        for existing_file in os.listdir(subdir_path):
                            if existing_file.lower() == file_name.lower() or existing_file == file_name:
                                filepath = os.path.join(subdir_path, existing_file)
                                print(f"   🔄 Found matching file: {filepath}")
                                break
                    break
    
    if not os.path.exists(filepath):
        # Debug: list what's actually in the processed folder
        print(f"   ❌ FILE NOT FOUND!")
        print(f"   Contents of PROCESSED_FOLDER:")
        for item in os.listdir(PROCESSED_FOLDER):
            item_path = os.path.join(PROCESSED_FOLDER, item)
            if os.path.isdir(item_path):
                print(f"      📁 {item}/")
                for subitem in os.listdir(item_path)[:5]:
                    print(f"         - {subitem}")
            else:
                print(f"      📄 {item}")
        abort(404)
    
    # Use send_file with absolute path (most reliable)
    print(f"   ✅ Sending file: {filepath}")
    
    # Get clean filename for download
    download_filename = os.path.basename(filepath)
    
    # Read file into memory first so we can delete it after
    with open(filepath, 'rb') as f:
        file_data = f.read()
    
    from io import BytesIO
    
    # Create response from memory
    response = send_file(
        BytesIO(file_data),
        as_attachment=True,
        download_name=download_filename,
        mimetype='audio/mpeg' if filepath.endswith('.mp3') else 'audio/wav'
    )
    
    # Add CORS headers for cross-origin downloads
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Expose-Headers'] = 'Content-Disposition'
    
    # Log the download
    if track_name:
        log_file_download(track_name, filepath)
    
    # ==========================================================================
    # DELAYED DELETE: Schedule track deletion after X minutes
    # ==========================================================================
    if DELAYED_DELETE_ENABLED and track_name:
        schedule_track_deletion(track_name)
    
    # ==========================================================================
    # SEQUENTIAL MODE: Track individual file downloads
    # Delete track ONLY after ALL versions (MP3 + WAV) have been downloaded
    # ==========================================================================
    if SEQUENTIAL_MODE and track_name:
        print(f"   📊 SEQUENTIAL MODE: Tracking download")
        print(f"      Track name: '{track_name}'")
        print(f"      File downloaded: '{download_filename}'")
        all_done = mark_file_downloaded(track_name, download_filename)
        
        # Add download status to response headers for frontend tracking
        remaining = get_pending_downloads_for_track(track_name)
        response.headers['X-Files-Remaining'] = str(len(remaining))
        response.headers['X-All-Downloaded'] = 'true' if all_done else 'false'
        response.headers['X-Track-Name'] = track_name
        
        if all_done:
            # ALL files for this track have been downloaded - cleanup now!
            print(f"   🎉 ALL FILES DOWNLOADED for '{track_name}' - cleaning up...")
            try:
                # Delete the entire track folder
                track_folder = os.path.join(PROCESSED_FOLDER, track_name)
                if os.path.exists(track_folder):
                    shutil.rmtree(track_folder)
                    print(f"   🗑️ Deleted track folder: {track_folder}")
                
                # Clean up htdemucs intermediate files
                htdemucs_folder = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
                if os.path.exists(htdemucs_folder):
                    shutil.rmtree(htdemucs_folder)
                    print(f"   🗑️ Deleted htdemucs folder: {htdemucs_folder}")
                
                # Remove from pending downloads tracker
                cleanup_track_after_downloads(track_name)
                
                log_message(f"✅ Track fully downloaded and cleaned: {track_name}")
            except Exception as e:
                print(f"   ⚠️ Cleanup error: {e}")
        else:
            print(f"   📥 {len(remaining)} files still pending for '{track_name}'")
    
    # Legacy DELETE_AFTER_DOWNLOAD mode (individual file deletion)
    elif DELETE_AFTER_DOWNLOAD and not SEQUENTIAL_MODE:
        try:
            # Delete the specific file that was downloaded
            os.unlink(filepath)
            print(f"   🗑️ Deleted after download: {filepath}")
            
            # Check if the track folder is now empty, if so delete it too
            if track_name:
                track_folder = os.path.join(PROCESSED_FOLDER, track_name)
                if os.path.exists(track_folder) and os.path.isdir(track_folder):
                    remaining_files = os.listdir(track_folder)
                    if len(remaining_files) == 0:
                        shutil.rmtree(track_folder)
                        print(f"   🗑️ Deleted empty folder: {track_folder}")
                        
                        # Also clean up htdemucs intermediate files
                        htdemucs_folder = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
                        if os.path.exists(htdemucs_folder):
                            shutil.rmtree(htdemucs_folder)
                            print(f"   🗑️ Deleted htdemucs folder: {htdemucs_folder}")
                        
                        # Remove from pending downloads
                        confirm_track_download(track_name)
        except Exception as e:
            print(f"   ⚠️ Could not delete after download: {e}")
    
    return response

@app.route('/confirm_download', methods=['POST', 'GET'])
def confirm_download():
    """
    Endpoint for track.idbyrivoli.com to confirm successful download of a track.
    Once confirmed, all files for this track will be deleted.
    
    Expected payload (POST JSON):
    {
        "track_name": "Track Name"  // The track name (folder name in processed/)
    }
    
    Or via query params (GET or POST):
    /confirm_download?track_name=Track%20Name
    
    Or via form data (POST):
    track_name=Track%20Name
    """
    track_name = None
    
    # Debug: log the request details
    print(f"")
    print(f"🔔 CONFIRM_DOWNLOAD REQUEST RECEIVED:")
    print(f"   Method: {request.method}")
    print(f"   Content-Type: {request.content_type}")
    print(f"   Query params: {dict(request.args)}")
    print(f"   Is JSON: {request.is_json}")
    
    # Try to get track_name from multiple sources (most flexible)
    
    # 1. Check query params first (works for both GET and POST)
    track_name = request.args.get('track_name') or request.args.get('trackName')
    
    # 2. Check JSON body
    if not track_name and request.is_json:
        try:
            data = request.get_json(force=False, silent=True)
            if data:
                track_name = data.get('track_name') or data.get('trackName')
                print(f"   JSON body: {data}")
        except Exception as e:
            print(f"   JSON parse error: {e}")
    
    # 3. Check form data
    if not track_name and request.form:
        track_name = request.form.get('track_name') or request.form.get('trackName')
        print(f"   Form data: {dict(request.form)}")
    
    # 4. Try to parse raw body as JSON (for cases where Content-Type is wrong)
    if not track_name and request.data:
        try:
            import json
            data = json.loads(request.data.decode('utf-8'))
            track_name = data.get('track_name') or data.get('trackName')
            print(f"   Parsed raw body as JSON: {data}")
        except:
            print(f"   Raw body (not JSON): {request.data[:200] if request.data else 'empty'}")
    
    print(f"   Extracted track_name: '{track_name}'")
    
    if not track_name:
        print(f"   ❌ ERROR: track_name is missing!")
        return jsonify({
            'error': 'track_name is required',
            'hint': 'Send as JSON body {"track_name": "..."} or query param ?track_name=...',
            'received': {
                'query_params': dict(request.args),
                'content_type': request.content_type,
                'method': request.method
            }
        }), 400
    
    # URL decode track name (in case it's encoded)
    track_name = urllib.parse.unquote(track_name)
    
    print(f"")
    print(f"🔔 ════════════════════════════════════════════════")
    print(f"🔔 CONFIRM DOWNLOAD REQUEST: '{track_name}'")
    print(f"🔔 From: {request.remote_addr}")
    print(f"🔔 ════════════════════════════════════════════════")
    
    # Check both tracking systems
    in_pending_downloads = track_name in pending_downloads
    in_sequential_tracking = track_name in track_download_status
    
    print(f"   In pending_downloads: {in_pending_downloads}")
    print(f"   In track_download_status (sequential): {in_sequential_tracking}")
    
    # SEQUENTIAL MODE: If track is in sequential tracking, trigger cleanup
    if SEQUENTIAL_MODE and in_sequential_tracking:
        # Mark all files as downloaded and cleanup
        with track_download_status_lock:
            if track_name in track_download_status:
                # Mark all files as downloaded
                for f in track_download_status[track_name]['files']:
                    track_download_status[track_name]['files'][f] = True
                track_download_status[track_name]['all_downloaded'] = True
        
        # Trigger cleanup
        cleanup_track_after_downloads(track_name)
        log_message(f"📥 Téléchargement confirmé (sequential): {track_name}")
        
        return jsonify({
            'success': True,
            'message': f"Track '{track_name}' confirmed and cleaned up (sequential mode)",
            'pending_count': get_pending_tracks_count()
        })
    
    # Legacy mode: Schedule for deletion after delay
    if schedule_track_deletion(track_name):
        log_message(f"📥 Téléchargement confirmé: {track_name} (suppression dans {DELETION_DELAY_MINUTES}min)")
        return jsonify({
            'success': True,
            'message': f"Track '{track_name}' confirmed, will be deleted in {DELETION_DELAY_MINUTES} minutes",
            'deletion_delay_minutes': DELETION_DELAY_MINUTES,
            'pending_count': get_pending_tracks_count()
        })
    
    # Track not found in either system - try to find similar names
    similar_tracks = []
    with track_download_status_lock:
        for name in track_download_status.keys():
            if track_name.lower() in name.lower() or name.lower() in track_name.lower():
                similar_tracks.append(f"sequential: {name}")
    with pending_downloads_lock:
        for name in pending_downloads.keys():
            if track_name.lower() in name.lower() or name.lower() in track_name.lower():
                similar_tracks.append(f"pending: {name}")
    
    log_message(f"⚠️ Confirmation échouée: {track_name} (non trouvé)")
    return jsonify({
        'success': False,
        'error': f"Track '{track_name}' not found",
        'similar_tracks': similar_tracks[:5] if similar_tracks else [],
        'hint': 'Track name must match exactly (case-sensitive)',
        'pending_count': get_pending_tracks_count()
    }), 404


@app.route('/track_download_status')
def get_track_download_status_endpoint():
    """
    Get download status for a specific track in sequential mode.
    Shows which files have been downloaded and which are still pending.
    """
    track_name = request.args.get('track_name')
    
    if not track_name:
        # Return status for all tracks with pending downloads
        with track_download_status_lock:
            all_statuses = {}
            for name, status in track_download_status.items():
                files = status['files']
                downloaded = sum(1 for v in files.values() if v)
                total = len(files)
                all_statuses[name] = {
                    'downloaded_count': downloaded,
                    'total_count': total,
                    'all_downloaded': status['all_downloaded'],
                    'pending_files': [f for f, d in files.items() if not d]
                }
            return jsonify({
                'sequential_mode': SEQUENTIAL_MODE,
                'tracks': all_statuses
            })
    
    # URL decode track name
    track_name = urllib.parse.unquote(track_name)
    status = get_track_download_status(track_name)
    
    if not status:
        return jsonify({'error': f"Track '{track_name}' not found"}), 404
    
    files = status['files']
    downloaded = sum(1 for v in files.values() if v)
    total = len(files)
    
    return jsonify({
        'track_name': track_name,
        'downloaded_count': downloaded,
        'total_count': total,
        'all_downloaded': status['all_downloaded'],
        'files': {f: 'downloaded' if d else 'pending' for f, d in files.items()},
        'pending_files': [f for f, d in files.items() if not d]
    })


@app.route('/already_processed')
def list_already_processed():
    """
    List all tracks that have already been processed.
    These tracks will be skipped when re-uploaded.
    """
    processed_tracks = get_already_processed_tracks()
    
    # Get detailed info for each track
    tracks_info = []
    for track_name in processed_tracks:
        track_folder = os.path.join(PROCESSED_FOLDER, track_name)
        files = []
        if os.path.exists(track_folder):
            files = [f for f in os.listdir(track_folder) if f.endswith(('.mp3', '.wav'))]
        
        tracks_info.append({
            'track_name': track_name,
            'files_count': len(files),
            'files': files[:10]  # Limit to first 10 for response size
        })
    
    return jsonify({
        'count': len(processed_tracks),
        'tracks': tracks_info
    })

@app.route('/pending_downloads')
def list_pending_downloads():
    """
    List all tracks pending download confirmation.
    Useful for monitoring and debugging.
    """
    # Check for API key (optional - can be public for monitoring)
    auth_header = request.headers.get('Authorization', '')
    api_key_param = request.args.get('api_key', 'idbyrivoli-secret-key-2024')
    
    is_authenticated = (
        auth_header == f'Bearer {API_KEY}' or 
        api_key_param == API_KEY
    )
    
    pending = get_pending_tracks_list()
    warning = check_pending_tracks_warning()
    
    return jsonify({
        'pending_count': len(pending),
        'max_pending': MAX_PENDING_TRACKS,
        'warning_threshold': PENDING_WARNING_THRESHOLD,
        'warning': warning,
        'tracks': pending if is_authenticated else [{'track_name': t['track_name'], 'age_hours': t['age_hours']} for t in pending]
    })


# Serve static files from processed folder directly
@app.route('/processed/<path:filepath>')
def serve_processed_file(filepath):
    """Alternative route: serve files directly from processed folder."""
    full_path = os.path.join(PROCESSED_FOLDER, filepath)
    print(f"📥 SERVE PROCESSED: {filepath}")
    print(f"   Full path: {full_path}")
    print(f"   Exists: {os.path.exists(full_path)}")
    
    if not os.path.exists(full_path):
        abort(404)
    
    return send_file(full_path, as_attachment=True)

# Debug route to list all processed files
@app.route('/list_files')
def list_files():
    """Debug route to see what files are available."""
    result = {}
    for subdir in os.listdir(PROCESSED_FOLDER):
        subdir_path = os.path.join(PROCESSED_FOLDER, subdir)
        if os.path.isdir(subdir_path):
            result[subdir] = os.listdir(subdir_path)
    return jsonify(result)

# =============================================================================
# DATABASE MODE STATUS
# =============================================================================

@app.route('/database_status')
def database_status():
    """Get the current database mode status and connection info."""
    status = {
        'database_mode_enabled': USE_DATABASE_MODE,
        'api_endpoint': API_ENDPOINT if not USE_DATABASE_MODE else None,
    }
    
    if USE_DATABASE_MODE:
        try:
            from database_service import check_database_connection, get_database_service, get_schema_info, test_database_insert
            db = get_database_service()
            connected = check_database_connection()
            status['database_connected'] = connected
            status['database_host'] = os.environ.get('DATABASE_HOST', 'from DATABASE_URL')
            status['database_name'] = os.environ.get('DATABASE_NAME', 'from DATABASE_URL')
            status['database_url_set'] = bool(os.environ.get('DATABASE_URL'))
            
            if connected:
                # Get schema info
                schema_info = get_schema_info()
                status['schema'] = {
                    'track_table_exists': schema_info.get('track_table_exists', False),
                    'tables_count': len(schema_info.get('tables', [])),
                    'track_columns_count': len(schema_info.get('track_columns', [])),
                }
                
                # Show some track columns for debugging
                track_cols = schema_info.get('track_columns', [])
                status['schema']['sample_columns'] = [c['column_name'] for c in track_cols[:20]]
                
                # Test insert capability
                test_result = test_database_insert()
                status['insert_test'] = test_result
                
        except Exception as e:
            status['database_connected'] = False
            status['database_error'] = str(e)
            import traceback
            status['traceback'] = traceback.format_exc()
    else:
        status['database_connected'] = False
        status['note'] = 'Database mode disabled - using external API'
    
    return jsonify(status)

# =============================================================================
# BATCH PROCESSING ROUTES
# =============================================================================

@app.route('/batch_status')
def get_batch_status():
    """Get the current batch processing status including disk info."""
    disk_usage = get_disk_usage_percent()
    
    # Count total tracks in processed folder
    total_tracks = 0
    try:
        if os.path.exists(PROCESSED_FOLDER):
            total_tracks = len([d for d in os.listdir(PROCESSED_FOLDER) if os.path.isdir(os.path.join(PROCESSED_FOLDER, d))])
    except:
        pass
    
    with batch_lock:
        return jsonify({
            'enabled': BATCH_MODE_ENABLED,
            'milestone_size': BATCH_SIZE,
            'processed_count': batch_processed_count,
            'queue_size': track_queue.qsize(),
            'pending_downloads': get_pending_tracks_count(),
            'sequential_tracks': len(track_download_status),
            'continuous_processing': True,  # No pause, no auto-delete
            'disk': {
                'usage_percent': disk_usage,
                'threshold_percent': DISK_THRESHOLD_PERCENT,
                'cleanup_enabled': DISK_CLEANUP_ENABLED,
                'tracks_to_delete': TRACKS_TO_DELETE,
                'total_tracks_stored': total_tracks,
                'cleanup_in_progress': disk_cleanup_in_progress
            }
        })

@app.route('/batch_cleanup', methods=['POST'])
def manual_batch_cleanup():
    """Manually trigger disk-based cleanup (delete oldest 25k tracks)."""
    log_message(f"🗑️ Manual disk cleanup triggered")
    
    # Use the disk-based cleanup function
    deleted_count = delete_oldest_tracks(TRACKS_TO_DELETE)
    
    log_message(f"✅ Manual cleanup complete: {deleted_count} oldest tracks deleted")
    
    return jsonify({
        'success': True,
        'deleted_count': deleted_count,
        'disk_usage_percent': get_disk_usage_percent(),
        'message': f'Deleted {deleted_count} oldest tracks'
    })

@app.route('/cleanup_oldest', methods=['POST'])
def cleanup_oldest_tracks():
    """Manually trigger cleanup of oldest N tracks (custom count via query param)."""
    count = request.args.get('count', TRACKS_TO_DELETE, type=int)
    
    # Cap at reasonable maximum
    count = min(count, 100000)
    
    log_message(f"🗑️ Manual cleanup of {count} oldest tracks triggered")
    
    deleted_count = delete_oldest_tracks(count)
    
    return jsonify({
        'success': True,
        'requested_count': count,
        'deleted_count': deleted_count,
        'disk_usage_percent': get_disk_usage_percent(),
        'message': f'Deleted {deleted_count} oldest tracks'
    })

@app.route('/batch_reset', methods=['POST'])
def reset_batch_counter():
    """Reset the batch counter without cleanup."""
    global batch_processed_count
    with batch_lock:
        old_count = batch_processed_count
        batch_processed_count = 0
    
    log_message(f"🔄 Batch counter reset: {old_count} → 0")
    
    return jsonify({
        'success': True,
        'old_count': old_count,
        'new_count': 0
    })

@app.route('/scheduled_deletions')
def get_scheduled_deletions():
    """Get list of tracks scheduled for delayed deletion."""
    with scheduled_deletions_lock:
        deletions = []
        current_time = time.time()
        for track_name, scheduled_time in scheduled_deletions.items():
            elapsed = current_time - scheduled_time
            remaining = max(0, (DELAYED_DELETE_MINUTES * 60) - elapsed)
            deletions.append({
                'track': track_name,
                'scheduled_at': scheduled_time,
                'elapsed_seconds': int(elapsed),
                'remaining_seconds': int(remaining),
                'remaining_minutes': round(remaining / 60, 1)
            })
    
    return jsonify({
        'enabled': DELAYED_DELETE_ENABLED,
        'delay_minutes': DELAYED_DELETE_MINUTES,
        'scheduled_count': len(deletions),
        'scheduled_deletions': deletions
    })

# =============================================================================
# UPLOAD HISTORY ROUTES
# =============================================================================

@app.route('/history')
def get_history():
    """Get the upload history as JSON."""
    history = get_upload_history_list()
    
    # Calculate stats
    total = len(history)
    completed = sum(1 for h in history if h['status'] == 'completed')
    failed = sum(1 for h in history if h['status'] == 'failed')
    pending = sum(1 for h in history if h['status'] in ['uploaded', 'processing'])
    
    return jsonify({
        'history': history,
        'stats': {
            'total': total,
            'completed': completed,
            'failed': failed,
            'pending': pending
        }
    })

@app.route('/history/csv')
def download_history_csv():
    """Download the upload history as a CSV file."""
    history = get_upload_history_list()
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['filename', 'status', 'date', 'type', 'session_id', 'error'])
    writer.writeheader()
    
    for entry in history:
        writer.writerow(entry)
    
    # Create response
    output.seek(0)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=upload_history_{timestamp}.csv'}
    )

@app.route('/history/clear', methods=['POST'])
def clear_history():
    """Clear the upload history."""
    clear_upload_history()
    log_message("🗑️ Upload history cleared")
    return jsonify({'message': 'History cleared'})

# Debug route to check detected public URL
@app.route('/debug_url')
def debug_url():
    """Debug route to see the detected public URL and request headers."""
    return jsonify({
        'CURRENT_HOST_URL': CURRENT_HOST_URL,
        'PUBLIC_URL_ENV': os.environ.get('PUBLIC_URL', ''),
        'headers': {
            'Host': request.headers.get('Host'),
            'X-Forwarded-Host': request.headers.get('X-Forwarded-Host'),
            'X-Forwarded-Proto': request.headers.get('X-Forwarded-Proto'),
            'X-Real-IP': request.headers.get('X-Real-IP'),
        },
        'request_host': request.host,
        'request_url': request.url,
    })

@app.route('/debug_cleanup')
def debug_cleanup():
    """Debug route to check pending downloads and scheduled deletions status."""
    pending = get_pending_tracks_list()
    warning = check_pending_tracks_warning()
    
    # Get scheduled deletions info
    now = time.time()
    scheduled_info = []
    with scheduled_deletions_lock:
        for track_name, info in scheduled_deletions.items():
            time_remaining = max(0, info['delete_after'] - now)
            scheduled_info.append({
                'track_name': track_name,
                'scheduled_at': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(info['scheduled_at'])),
                'delete_after': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(info['delete_after'])),
                'seconds_remaining': int(time_remaining),
                'minutes_remaining': round(time_remaining / 60, 1)
            })
    
    return jsonify({
        'mode': 'confirmation_based_with_delay',
        'description': f'Files stay until /confirm_download is called, then deleted after {DELETION_DELAY_MINUTES} minutes. All files auto-deleted after {MAX_FILE_AGE_HOURS} hours.',
        'settings': {
            'deletion_delay_minutes': DELETION_DELAY_MINUTES,
            'max_file_age_hours': MAX_FILE_AGE_HOURS,
            'cleanup_interval_minutes': CLEANUP_INTERVAL_MINUTES,
            'delete_after_download': DELETE_AFTER_DOWNLOAD
        },
        'pending_count': len(pending),
        'scheduled_deletion_count': len(scheduled_info),
        'max_pending': MAX_PENDING_TRACKS,
        'warning_threshold': PENDING_WARNING_THRESHOLD,
        'warning': warning,
        'pending_tracks': pending,
        'scheduled_deletions': scheduled_info,
        'current_time': time.strftime("%Y-%m-%d %H:%M:%S"),
        'endpoints': {
            'confirm_download': f'POST /confirm_download with track_name and api_key (triggers {DELETION_DELAY_MINUTES}min deletion delay)',
            'list_pending': 'GET /pending_downloads'
        }
    })

@app.route('/debug_gpu')
def debug_gpu():
    """Debug route to check GPU/CUDA status."""
    info = {
        'demucs_device': DEMUCS_DEVICE,
        'cuda_available': False,
        'cuda_version': None,
        'pytorch_version': None,
        'gpu_name': None,
        'gpu_memory_gb': None,
        'gpu_count': 0,
        'error': None
    }
    
    try:
        import torch
        info['pytorch_version'] = torch.__version__
        info['cuda_available'] = torch.cuda.is_available()
        
        if hasattr(torch.version, 'cuda'):
            info['cuda_version'] = torch.version.cuda
        
        if torch.cuda.is_available():
            info['gpu_count'] = torch.cuda.device_count()
            if info['gpu_count'] > 0:
                info['gpu_name'] = torch.cuda.get_device_name(0)
                props = torch.cuda.get_device_properties(0)
                info['gpu_memory_gb'] = round(props.total_memory / (1024**3), 1)
                
                # Current memory usage
                info['gpu_memory_allocated_gb'] = round(torch.cuda.memory_allocated(0) / (1024**3), 2)
                info['gpu_memory_reserved_gb'] = round(torch.cuda.memory_reserved(0) / (1024**3), 2)
    except Exception as e:
        info['error'] = str(e)
    
    return jsonify(info)

# Test route to check URL generation
@app.route('/test_download')
def test_download():
    """Test route that lists all files with their download URLs and tests them."""
    results = []
    
    for subdir in os.listdir(PROCESSED_FOLDER):
        subdir_path = os.path.join(PROCESSED_FOLDER, subdir)
        if os.path.isdir(subdir_path):
            for filename in os.listdir(subdir_path):
                file_path = os.path.join(subdir_path, filename)
                rel_path = f"{subdir}/{filename}"
                url = f"/download_file?path={urllib.parse.quote(rel_path, safe='/')}"
                
                # Test if the path would work
                test_path = os.path.join(PROCESSED_FOLDER, rel_path)
                
                results.append({
                    'subdir': subdir,
                    'filename': filename,
                    'rel_path': rel_path,
                    'url': url,
                    'file_exists_at_original': os.path.exists(file_path),
                    'file_exists_at_test_path': os.path.exists(test_path),
                    'paths_match': file_path == test_path
                })
    
    return jsonify({
        'PROCESSED_FOLDER': PROCESSED_FOLDER,
        'total_files': len(results),
        'files': results
    })

@app.route('/cleanup', methods=['POST'])
def cleanup_files():
    """
    Deletes all files in uploads, output, and processed directories to free up disk space.
    Also clears all in-memory state to start fresh.
    """
    global job_status, upload_history
    
    try:
        # Clear directories
        for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER, PROCESSED_FOLDER]:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')
        
        # Also clear covers folder (extracted covers)
        covers_folder = os.path.join(BASE_DIR, 'static', 'covers')
        for filename in os.listdir(covers_folder):
            if filename.startswith('cover_'):  # Only delete extracted covers, not the main one
                file_path = os.path.join(covers_folder, filename)
                try:
                    os.unlink(file_path)
                except:
                    pass

        # Reset Job Status COMPLETELY
        job_status = {
            'state': 'idle', 
            'progress': 0,
            'total_files': 0,
            'current_file_idx': 0,
            'current_filename': '',
            'current_step': '',
            'results': [],  # IMPORTANT: Clear results
            'error': None,
            'logs': [],
            'queue_size': 0
        }
        
        # Clear Queue (drain it)
        with track_queue.mutex:
            track_queue.queue.clear()
        
        # Clear queue tracker
        with queue_items_lock:
            queue_items.clear()
        
        # Clear pending downloads
        with pending_downloads_lock:
            pending_downloads.clear()
        
        # Clear scheduled deletions
        with scheduled_deletions_lock:
            scheduled_deletions.clear()
            
        print("🧹 FULL RESET: All files, queues, and pending downloads cleared")
        return jsonify({'message': 'Cleanup successful', 'results_cleared': True})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def kill_jupyter():
    """Kill any running Jupyter processes to free up resources."""
    try:
        import signal
        result = subprocess.run(['pgrep', '-f', 'jupyter'], capture_output=True, text=True)
        pids = result.stdout.strip().split('\n')
        killed = 0
        for pid in pids:
            if pid:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    killed += 1
                except (ProcessLookupError, ValueError):
                    pass
        if killed > 0:
            print(f"🔪 Killed {killed} Jupyter process(es)")
    except Exception as e:
        print(f"⚠️ Could not kill Jupyter: {e}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ID By Rivoli Audio Processor')
    parser.add_argument('-p', '--port', type=int, default=int(os.environ.get('PORT', 8888)),
                        help='Port to run the server on (default: 8888)')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debug mode (development only)')
    parser.add_argument('--dev', action='store_true', default=False,
                        help='Run in development mode with Flask dev server')
    args = parser.parse_args()
    
    # Kill Jupyter processes before starting
    kill_jupyter()
    
    if args.dev or args.debug:
        # Development mode with Flask dev server
        print(f"🔧 Starting ID By Rivoli in DEVELOPMENT mode on port {args.port}")
        app.run(host='0.0.0.0', port=args.port, debug=True)
    else:
        # Production mode - recommend using Gunicorn
        print(f"🚀 Starting ID By Rivoli in PRODUCTION mode on port {args.port}")
        print(f"💡 Tip: For better performance, use: gunicorn -c gunicorn_config.py app:app")
        app.run(host='0.0.0.0', port=args.port, debug=False, threaded=True)
