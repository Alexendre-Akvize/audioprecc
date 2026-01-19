import os
import subprocess
import threading
import shutil
import time
import re
import zipfile
import io
from flask import Flask, render_template, request, jsonify, send_from_directory, abort, send_file, session
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

# Maximum number of pending tracks before warning (configurable)
# Set high to support batch uploads of 1000+ tracks
MAX_PENDING_TRACKS = int(os.environ.get('MAX_PENDING_TRACKS', 1500))
PENDING_WARNING_THRESHOLD = int(os.environ.get('PENDING_WARNING_THRESHOLD', 1000))

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

def track_file_for_pending_download(track_name, original_path, num_files=6):
    """Register a track as pending download - files will stay until track.idbyrivoli.com confirms download."""
    with pending_downloads_lock:
        # Also track the htdemucs intermediate folder
        htdemucs_dir = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
        pending_downloads[track_name] = {
            'files_total': num_files,
            'files': [],  # Will be populated with file paths
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
        print(f"📝 Status: AWAITING download from track.idbyrivoli.com")
        print(f"📝 Total pending tracks: {pending_count}")
        if pending_count >= PENDING_WARNING_THRESHOLD:
            print(f"📝 ⚠️ WARNING: {pending_count} tracks pending (threshold: {PENDING_WARNING_THRESHOLD})")
        print(f"📝 ════════════════════════════════════════════════")

def confirm_track_download(track_name):
    """
    Called when track.idbyrivoli.com confirms successful download.
    Deletes all files associated with this track.
    Returns True if track was found and deleted, False otherwise.
    """
    with pending_downloads_lock:
        if track_name not in pending_downloads:
            print(f"⚠️ Track '{track_name}' not found in pending downloads")
            return False
        
        track_info = pending_downloads[track_name]
        print(f"")
        print(f"✅ ════════════════════════════════════════════════")
        print(f"✅ CONFIRMED DOWNLOAD: '{track_name}'")
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
        
        # Remove from pending downloads
        del pending_downloads[track_name]
        print(f"   ✅ Cleanup complete for '{track_name}'")
        print(f"   📊 Remaining pending tracks: {len(pending_downloads)}")
        print(f"✅ ════════════════════════════════════════════════")
        
        return True

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
                'session_id': session_id
            }
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
    scheme = request.headers.get('X-Forwarded-Proto', 'https')  # Default to https for pods
    
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
    Sends track information to external API endpoint with authentication.
    """
    if not API_ENDPOINT:
        print("⚠️  API_ENDPOINT not configured, skipping API call")
        return
    
    try:
        import json
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {API_KEY}'
        }
        
        # Log the full payload being sent
        print(f"\n{'='*60}")
        print(f"📤 API PAYLOAD for: {track_data.get('Titre', 'N/A')} ({track_data.get('Format', 'N/A')})")
        print(f"{'='*60}")
        print(json.dumps(track_data, indent=2, ensure_ascii=False))
        print(f"{'='*60}\n")
        
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
    
    # Register track as pending download - files stay until track.idbyrivoli.com confirms
    # Count actual files: each edit has MP3 + WAV = 2 files per edit
    num_files = len(edits) * 2
    track_file_for_pending_download(metadata_base_name, original_path, num_files)

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

# No duplicate checking - all tracks are processed fresh each time

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

# Global Queue for processing tracks
track_queue = queue.Queue()

# Track queue items with status for UI display
# Structure: { "filename": { "status": "waiting|processing|done", "worker": None|worker_id, "progress": 0-100 } }
queue_items = {}
queue_items_lock = Lock()

def add_to_queue_tracker(filename, session_id):
    """Add item to queue tracker for UI display."""
    with queue_items_lock:
        queue_items[filename] = {
            'status': 'waiting',
            'worker': None,
            'progress': 0,
            'session_id': session_id,
            'step': 'En attente...'
        }

def update_queue_item(filename, status=None, worker=None, progress=None, step=None):
    """Update queue item status."""
    with queue_items_lock:
        if filename in queue_items:
            if status: queue_items[filename]['status'] = status
            if worker is not None: queue_items[filename]['worker'] = worker
            if progress is not None: queue_items[filename]['progress'] = progress
            if step: queue_items[filename]['step'] = step

def remove_from_queue_tracker(filename):
    """Remove item from queue tracker."""
    with queue_items_lock:
        if filename in queue_items:
            del queue_items[filename]

def get_queue_items_list():
    """Get list of queue items for UI."""
    with queue_items_lock:
        items = []
        for filename, info in queue_items.items():
            items.append({
                'filename': filename,
                'status': info['status'],
                'worker': info['worker'],
                'progress': info['progress'],
                'step': info['step']
            })
        # Sort: processing first, then waiting
        items.sort(key=lambda x: (0 if x['status'] == 'processing' else 1, x['filename']))
        return items

# Worker thread function
def worker(worker_id):
    while True:
        try:
            queue_item = track_queue.get()
            if queue_item is None:
                break
            
            # Handle both old format (string) and new format (dict with session_id)
            if isinstance(queue_item, dict):
                filename = queue_item['filename']
                session_id = queue_item.get('session_id', 'global')
            else:
                filename = queue_item
                session_id = 'global'
            
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
                log_message(f"⚠️ Fichier introuvable après 5 tentatives (ignoré) : {filename}", session_id)
                log_message(f"   Chemins vérifiés: {session_upload_folder}/{filename} et {UPLOAD_FOLDER}/{filename}", session_id)
                remove_from_queue_tracker(filename)
                track_queue.task_done()
                if track_queue.empty():
                    current_status['state'] = 'idle'
                    current_status['current_step'] = ''
                    current_status['current_filename'] = ''
                continue

            # Update tracker: now processing
            update_queue_item(filename, status='processing', worker=worker_id, progress=0, step='Démarrage...')
            
            print(f"🔄 Worker {worker_id} traite: {filename}")
            process_single_track(filepath, filename, session_id, worker_id)
            
            # Remove from tracker when done
            remove_from_queue_tracker(filename)
            track_queue.task_done()
            
            # Reset state to idle if queue is empty
            if track_queue.empty():
                current_status['state'] = 'idle'
                current_status['current_step'] = 'Prêt pour de nouveaux fichiers'
                current_status['current_filename'] = ''
                log_message("✅ File d'attente terminée - Prêt pour de nouveaux fichiers", session_id)
                
        except Exception as e:
            print(f"Worker {worker_id} Error: {e}")
            log_message(f"Erreur Worker {worker_id}: {e}")

# Start multiple worker threads
worker_threads = []
for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(i+1,), daemon=True)
    t.start()
    worker_threads.append(t)
print(f"🚀 {NUM_WORKERS} workers démarrés")

# Restore pending files on startup
def restore_queue():
    """Scans upload folder and re-queues any MP3 files found."""
    log_message("🔄 Vérification des fichiers en attente...")
    upload_files = [f for f in os.listdir(UPLOAD_FOLDER) if f.lower().endswith('.mp3')]
    
    count = 0
    for f in upload_files:
        track_queue.put(f)
        count += 1
            
    if count > 0:
        log_message(f"♻️ Restauration de {count} fichiers dans la file d'attente.")

# Call restore on startup
restore_queue()

# Modified process function for SINGLE track
def process_single_track(filepath, filename, session_id='global', worker_id=None):
    # Get session-specific status
    current_status = get_job_status(session_id)
    
    # Ensure CUDA is available (re-check in case it wasn't ready at startup)
    device = ensure_cuda_device()
    
    try:
        current_status['state'] = 'processing'
        current_status['current_filename'] = filename
        current_status['current_step'] = "Séparation IA (Demucs)..."
        log_message(f"🚀 [{session_id}] Début traitement : {filename} (Device: {device})", session_id)
        
        # Update queue tracker
        update_queue_item(filename, status='processing', worker=worker_id, progress=0, step=f'Séparation IA ({device})...')
        
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
                                update_queue_item(filename, progress=int(track_percent * 0.7), step=f'Séparation IA {track_percent}%')
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
            log_message(f"❌ Erreur Demucs pour {filename}")
            log_message(f"📋 Code retour: {returncode}")
            log_message(f"📋 Détails: {error_lines[:500]}")
            print(f"DEMUCS ERROR OUTPUT:\n{error_lines}")
            return
        
        # 2. Generate edits (Main, Acapella, Instrumental)
        current_status['current_step'] = "Génération des versions..."
        current_status['progress'] = 70
        update_queue_item(filename, progress=70, step='Export MP3/WAV...')
        
        clean_name, _ = clean_filename(filename)
        track_output_dir = os.path.join(PROCESSED_FOLDER, clean_name)
        os.makedirs(track_output_dir, exist_ok=True)
        
        # Get separated files
        source_dir = os.path.join(OUTPUT_FOLDER, 'htdemucs', track_name)
        inst_path = os.path.join(source_dir, 'no_vocals.mp3')
        vocals_path = os.path.join(source_dir, 'vocals.mp3')
        
        if os.path.exists(inst_path) and os.path.exists(vocals_path):
            update_queue_item(filename, progress=80, step='Création des versions...')
            edits = create_edits(vocals_path, inst_path, filepath, track_output_dir, filename)
            update_queue_item(filename, progress=100, step='Terminé')
            
            # Add to session-specific results
            current_status['results'].append({
                'original': clean_name,
                'edits': edits
            })
            log_message(f"✅ [{session_id}] Terminé : {clean_name}", session_id)
        else:
            log_message(f"⚠️ Fichiers séparés non trouvés pour {filename}", session_id)
        
        current_status['progress'] = 100

    except Exception as e:
        log_message(f"❌ Erreur critique {filename}: {e}", session_id)

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
    """
    # Acquire semaphore with timeout to prevent deadlocks
    acquired = UPLOAD_SEMAPHORE.acquire(timeout=300)  # 5 minute timeout
    if not acquired:
        return jsonify({'error': 'Server busy, too many concurrent uploads. Please retry.'}), 503
    
    try:
        session_id = get_session_id()
        
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
            
            print(f"📤 Upload: '{original_filename}' → '{safe_filename}'")
            
            # Use session-specific upload folder
            session_upload_folder = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
            os.makedirs(session_upload_folder, exist_ok=True)
            filepath = os.path.join(session_upload_folder, safe_filename)
            
            # Save file with explicit error handling
            try:
                file.save(filepath)
                print(f"✅ Saved: {safe_filename} ({os.path.getsize(filepath)} bytes)")
            except Exception as save_error:
                print(f"❌ Save error for {safe_filename}: {save_error}")
                import traceback
                traceback.print_exc()
                return jsonify({'error': f'Failed to save file: {str(save_error)}'}), 500
            
            return jsonify({
                'message': f'File {safe_filename} uploaded successfully', 
                'session_id': session_id,
                'filename': safe_filename,
                'original_filename': original_filename
            })
        
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
    
    # Return session-specific status
    return jsonify(current_status)

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
    
    # Log the download - file is NOT deleted here
    # Files stay available until track.idbyrivoli.com confirms via /confirm_download
    if track_name:
        log_file_download(track_name, filepath)
    
    return response

@app.route('/confirm_download', methods=['POST'])
def confirm_download():
    """
    Endpoint for track.idbyrivoli.com to confirm successful download of a track.
    Once confirmed, all files for this track will be deleted.
    
    Expected payload:
    {
        "track_name": "Track Name",  // The track name (folder name in processed/)
        "api_key": "your-api-key"    // Authentication
    }
    
    Or via query params:
    /confirm_download?track_name=Track%20Name&api_key=your-api-key
    """
    # Get track_name from JSON body or query params
    if request.is_json:
        data = request.get_json()
        track_name = data.get('track_name')
        provided_key = data.get('api_key')
    else:
        track_name = request.args.get('track_name')
        provided_key = request.args.get('api_key')
    
    # Also check Authorization header
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        provided_key = auth_header[7:]
    
    # Validate API key
    if provided_key != API_KEY:
        print(f"❌ Invalid API key for confirm_download: {track_name}")
        return jsonify({'error': 'Invalid API key'}), 401
    
    if not track_name:
        return jsonify({'error': 'track_name is required'}), 400
    
    # URL decode track name (in case it's encoded)
    track_name = urllib.parse.unquote(track_name)
    
    print(f"")
    print(f"🔔 ════════════════════════════════════════════════")
    print(f"🔔 CONFIRM DOWNLOAD REQUEST: '{track_name}'")
    print(f"🔔 From: {request.remote_addr}")
    print(f"🔔 ════════════════════════════════════════════════")
    
    # Confirm and delete
    if confirm_track_download(track_name):
        log_message(f"✅ Track téléchargé et supprimé: {track_name}")
        return jsonify({
            'success': True,
            'message': f"Track '{track_name}' confirmed and cleaned up",
            'pending_count': get_pending_tracks_count()
        })
    else:
        # Track not found - might already be deleted or never existed
        return jsonify({
            'success': False,
            'error': f"Track '{track_name}' not found in pending downloads",
            'pending_count': get_pending_tracks_count()
        }), 404


@app.route('/pending_downloads')
def list_pending_downloads():
    """
    List all tracks pending download confirmation.
    Useful for monitoring and debugging.
    """
    # Check for API key (optional - can be public for monitoring)
    auth_header = request.headers.get('Authorization', '')
    api_key_param = request.args.get('api_key', '')
    
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
    """Debug route to check pending downloads status."""
    pending = get_pending_tracks_list()
    warning = check_pending_tracks_warning()
    
    return jsonify({
        'mode': 'confirmation_based',
        'description': 'Files stay until track.idbyrivoli.com confirms download via /confirm_download',
        'pending_count': len(pending),
        'max_pending': MAX_PENDING_TRACKS,
        'warning_threshold': PENDING_WARNING_THRESHOLD,
        'warning': warning,
        'pending_tracks': pending,
        'current_time': time.strftime("%Y-%m-%d %H:%M:%S"),
        'endpoints': {
            'confirm_download': 'POST /confirm_download with track_name and api_key',
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
    global job_status, processed_history
    
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
            
        print("🧹 FULL RESET: All files and results cleared")
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
