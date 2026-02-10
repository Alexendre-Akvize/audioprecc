"""
Upload routes blueprint for IDByRivoli.

Handles file uploads, chunked uploads, direct uploads, and batch uploads.
"""
import os
import re
import time
import shutil
import uuid
import threading
import urllib.parse
from datetime import datetime

from flask import Blueprint, request, jsonify
from mutagen.mp3 import MP3
from mutagen.id3 import ID3

import config
from config import (
    UPLOAD_FOLDER,
    PROCESSED_FOLDER,
    UPLOAD_SEMAPHORE,
    BASE_DIR,
    track_queue,
    app,
)
from services.queue_service import (
    get_session_id,
    get_job_status,
    log_message,
    job_status,
    add_to_queue_tracker,
)
from services.track_service import run_demucs_thread
from utils.file_utils import (
    clean_filename,
    is_track_already_processed,
    format_artists,
    get_parent_label,
)
from utils.tracking import (
    check_pending_tracks_warning,
    get_pending_tracks_count,
)
from utils.history import (
    add_to_upload_history,
    update_upload_history_status,
)
from services.metadata_service import (
    detect_track_type_from_title,
    extract_bpm_from_filename,
    strip_trailing_bpm_and_key,
    clean_detected_type_from_title,
    search_deezer_metadata,
)

upload_bp = Blueprint('upload', __name__)


@upload_bp.route('/clear_results', methods=['POST'])
def clear_results():
    """Clears only the results list for current session (keeps files on disk)."""
    session_id = get_session_id()
    current_status = get_job_status(session_id)
    current_status['results'] = []
    current_status['logs'] = []
    log_message("🔄 Résultats vidés - prêt pour nouveaux tracks", session_id)
    return jsonify({'message': 'Results cleared', 'session_id': session_id})


@upload_bp.route('/enqueue_file', methods=['POST'])
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


@upload_bp.route('/upload_chunk', methods=['POST'])
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


@upload_bp.route('/start_processing', methods=['POST'])
def start_processing():
    """
    Triggered after all uploads are done.
    Scans the uploads folder and starts the processing thread.
    """
    if job_status['state'] == 'processing':
        return jsonify({'error': 'Un traitement est déjà en cours. Veuillez patienter.'}), 409

    # Scan upload folder for MP3s
    files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.lower().endswith('.mp3')]
    
    if not files:
        return jsonify({'error': 'Aucun fichier trouvé dans le dossier uploads'}), 400

    saved_filepaths = [os.path.join(app.config['UPLOAD_FOLDER'], f) for f in files]
    original_filenames = files  # filenames are just the basenames
    
    job_status['state'] = 'starting'
    job_status['progress'] = 0
    job_status['total_files'] = len(files)
    job_status['current_file_idx'] = 0
    job_status['current_filename'] = ''
    job_status['current_step'] = 'Initialisation...'
    job_status['results'] = []
    job_status['error'] = None
    job_status['logs'] = []
    
    log_message(f"Traitement démarré pour {len(files)} fichier(s) (Mode Batch)")
    
    thread = threading.Thread(target=run_demucs_thread, args=(saved_filepaths, original_filenames))
    thread.start()
    
    return jsonify({'message': 'Traitement démarré', 'total_files': len(files)})


@upload_bp.route('/upload', methods=['POST'])
def upload_file():
    # Keep legacy endpoint for backward compatibility if needed, 
    # but strictly we should move to the new flow.
    # ... (redirecting to new logic ideally, but let's keep it simple)
    return jsonify({'error': 'Please use the new sequential upload flow'}), 400


# =============================================================================
# UPLOAD ONLY MODE - Direct upload to S3 + Database (no processing)
# =============================================================================

@upload_bp.route('/upload_direct', methods=['POST'])
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
    if not config.USE_DATABASE_MODE:
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
    
    # Detect if file comes from deemix (only deemix tracks keep their original cover)
    source_folder = request.form.get('source', '')
    is_from_deemix = 'deemix' in (file.filename or '').lower() or 'deemix' in source_folder.lower()
    
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
                
                # Extract cover image ONLY for deemix tracks (they have correct covers)
                if is_from_deemix:
                    log_message(f"📂 [{session_id}] Deemix source — extracting original cover")
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
                                
                                base_url = config.CURRENT_HOST_URL if config.CURRENT_HOST_URL else ""
                                cover_url = f"{base_url}/static/covers/{cover_filename}"
                                log_message(f"   ✅ Cover extraite (deemix): {cover_filename}")
                                break
                            except:
                                pass
                else:
                    log_message(f"🚫 [{session_id}] Non-deemix source — skipping original cover extraction")
            
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
        
        # Remove trailing BPM and/or Camelot key from title (e.g., "10A 93", "1B", "102")
        original_title = strip_trailing_bpm_and_key(original_title)
        
        # Fallback: extract BPM from filename trailing number (DJ City format)
        if bpm is None:
            bpm = extract_bpm_from_filename(file.filename or safe_filename)
            if bpm:
                log_message(f"🎵 [{session_id}] BPM from filename: {bpm}")
        
        # ─── Deezer API: enrich metadata (ISRC, BPM, album, label, genre, cover) ───
        # Parse artist from filename for API search
        # extract_artist may not exist; use a safe fallback
        try:
            from services.metadata_service import extract_artist
            search_artist = artist if artist != 'Unknown' else extract_artist(file.filename or safe_filename)
        except (ImportError, AttributeError):
            search_artist = artist if artist != 'Unknown' else ''
        # Clean title for search (remove version markers, BPM, etc.)
        search_title = clean_detected_type_from_title(original_title or safe_filename)
        
        deezer_meta = {}
        try:
            log_message(f"🔍 [{session_id}] Searching Deezer: '{search_artist}' - '{search_title}'")
            deezer_meta = search_deezer_metadata(search_artist, search_title)
            if deezer_meta.get('deezer_id'):
                log_message(f"✅ [{session_id}] Deezer match (score {deezer_meta.get('match_score', 0.0):.2f}): {deezer_meta.get('artist')} - {deezer_meta.get('title')} (ISRC: {deezer_meta.get('isrc', 'N/A')})")
                
                # Fill missing fields from Deezer (ID3 tags take priority)
                if not isrc and deezer_meta.get('isrc'):
                    isrc = deezer_meta['isrc']
                    log_message(f"   📝 ISRC from Deezer: {isrc}")
                if bpm is None and deezer_meta.get('bpm'):
                    bpm = deezer_meta['bpm']
                    log_message(f"   📝 BPM from Deezer: {bpm}")
                if not album and deezer_meta.get('album'):
                    album = deezer_meta['album']
                    log_message(f"   📝 Album from Deezer: {album}")
                if not genre and deezer_meta.get('genre'):
                    genre = deezer_meta['genre']
                    log_message(f"   📝 Genre from Deezer: {genre}")
                if not sous_label and deezer_meta.get('label'):
                    sous_label = deezer_meta['label']
                    parent_label = get_parent_label(sous_label) if sous_label else ''
                    if parent_label == sous_label:
                        parent_label = ''
                    log_message(f"   📝 Label from Deezer: {sous_label}")
                if not date_sortie and deezer_meta.get('release_date'):
                    try:
                        date_obj = datetime.strptime(deezer_meta['release_date'][:10], '%Y-%m-%d')
                        date_sortie = int(date_obj.timestamp())
                        log_message(f"   📝 Release date from Deezer: {deezer_meta['release_date']}")
                    except:
                        pass
                
                # Cover from Deezer
                if deezer_meta.get('cover_url'):
                    cover_url = deezer_meta['cover_url']
                    log_message(f"   🖼️ Cover from Deezer: {cover_url[:80]}...")
            else:
                log_message(f"⚠️ [{session_id}] No Deezer match found")
        except Exception as e:
            log_message(f"⚠️ [{session_id}] Deezer lookup failed: {e}")
        
        # ─── FILTER: No Deezer match → skip entirely ───
        if not deezer_meta.get('deezer_id'):
            log_message(f"⏭️ [{session_id}] SKIPPED: No Deezer match for '{safe_filename}' - track not written to DB")
            # Clean up temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({
                'success': False,
                'skipped': True,
                'reason': 'No confident Deezer match found',
                'filename': safe_filename
            }), 200
        
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
        # The Titre field is passed to database_service which extracts the base title
        # and maps the type to the correct file field. Pass the full title with type info.
        title_lower = original_title.lower()
        type_already_in_title = (
            track_type.lower() in title_lower or
            (track_type == 'Acapella' and ('acapella' in title_lower or 'a capella' in title_lower or 'acappella' in title_lower)) or
            (track_type == 'Short' and ('quick hit' in title_lower or 'short' in title_lower)) or
            (track_type == 'Original Clean' and 'clean' in title_lower) or
            (track_type == 'Original Dirty' and 'dirty' in title_lower) or
            (track_type == 'Instrumental' and ('instrumental' in title_lower or 'inst]' in title_lower or 'inst)' in title_lower)) or
            (track_type == 'Intro' and 'intro' in title_lower)
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
        base_url = config.CURRENT_HOST_URL if config.CURRENT_HOST_URL else ""
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
        from database_service import save_track_to_database
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
            'TRACK_ID': track_id,
            '_force_cover_replace': bool(deezer_meta.get('cover_url')),
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


@upload_bp.route('/upload_direct_batch', methods=['POST'])
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
    if not config.USE_DATABASE_MODE:
        return jsonify({'error': 'Direct upload requires database mode to be enabled'}), 400
    
    session_id = get_session_id()
    
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    
    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': 'No files selected'}), 400
    
    fallback_track_type = request.form.get('track_type', 'Main')
    skip_waveform = request.form.get('skip_waveform', 'false').lower() == 'true'
    source_folder = request.form.get('source', '')
    
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
            
            # Fallback: extract BPM from filename trailing number (DJ City format)
            if bpm is None:
                bpm = extract_bpm_from_filename(file.filename or safe_filename)
            
            # Remove trailing BPM and/or Camelot key from title (e.g., "10A 93", "1B", "102")
            original_title = strip_trailing_bpm_and_key(original_title)
            
            # ─── Deezer API: enrich metadata ───
            try:
                from services.metadata_service import extract_artist
                search_artist = artist if artist != 'Unknown' else extract_artist(file.filename or safe_filename)
            except (ImportError, AttributeError):
                search_artist = artist if artist != 'Unknown' else ''
            search_title = clean_detected_type_from_title(original_title or safe_filename)
            
            deezer_meta = {}
            try:
                log_message(f"🔍 [{session_id}] Batch Deezer: '{search_artist}' - '{search_title}'")
                deezer_meta = search_deezer_metadata(search_artist, search_title)
                if deezer_meta.get('deezer_id'):
                    log_message(f"✅ [{session_id}] Deezer (score {deezer_meta.get('match_score', 0.0):.2f}): {deezer_meta.get('artist')} - {deezer_meta.get('title')}")
                    if not isrc and deezer_meta.get('isrc'):
                        isrc = deezer_meta['isrc']
                    if bpm is None and deezer_meta.get('bpm'):
                        bpm = deezer_meta['bpm']
                    if not album and deezer_meta.get('album'):
                        album = deezer_meta['album']
                    if not genre and deezer_meta.get('genre'):
                        genre = deezer_meta['genre']
                    if not sous_label and deezer_meta.get('label'):
                        sous_label = deezer_meta['label']
                        parent_label = get_parent_label(sous_label) if sous_label else ''
                        if parent_label == sous_label:
                            parent_label = ''
                    if not date_sortie and deezer_meta.get('release_date'):
                        try:
                            date_obj = datetime.strptime(deezer_meta['release_date'][:10], '%Y-%m-%d')
                            date_sortie = int(date_obj.timestamp())
                        except:
                            pass
                    if deezer_meta.get('cover_url'):
                        cover_url = deezer_meta['cover_url']
            except Exception as e:
                log_message(f"⚠️ [{session_id}] Deezer batch error: {e}")
            
            # ─── FILTER: No Deezer match → skip entirely ───
            if not deezer_meta.get('deezer_id'):
                log_message(f"⏭️ [{session_id}] SKIPPED (batch): No Deezer match for '{safe_filename}'")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                results.append({
                    'filename': safe_filename, 'success': False,
                    'skipped': True, 'reason': 'No confident Deezer match found'
                })
                error_count += 1
                continue
            
            # Auto-detect track type from title/filename
            detected_type = detect_track_type_from_title(original_title)
            if not detected_type:
                detected_type = detect_track_type_from_title(safe_filename)
            
            # Use detected type, or fallback to provided/default
            track_type = detected_type if detected_type else fallback_track_type
            
            # Build track title
            # The Titre field is passed to database_service which extracts the base title
            title_lower = original_title.lower()
            type_already_in_title = (
                track_type.lower() in title_lower or
                (track_type == 'Acapella' and ('acapella' in title_lower or 'a capella' in title_lower or 'acappella' in title_lower)) or
                (track_type == 'Short' and ('quick hit' in title_lower or 'short' in title_lower)) or
                (track_type == 'Original Clean' and 'clean' in title_lower) or
                (track_type == 'Original Dirty' and 'dirty' in title_lower) or
                (track_type == 'Instrumental' and ('instrumental' in title_lower or 'inst]' in title_lower or 'inst)' in title_lower)) or
                (track_type == 'Intro' and 'intro' in title_lower)
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
            base_url = config.CURRENT_HOST_URL if config.CURRENT_HOST_URL else ""
            absolute_url = f"{base_url}{file_url}"
            
            # Generate track ID
            filename_clean = original_title.replace('-', ' ').replace('_', ' ')
            filename_clean = re.sub(r'\s+', ' ', filename_clean).strip()
            filename_clean = filename_clean.replace(' ', '_')
            filename_clean = re.sub(r'_+', '_', filename_clean)
            track_id = f"{isrc}_{filename_clean}" if isrc else filename_clean
            
            # Prepare track data
            from database_service import save_track_to_database
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
                'TRACK_ID': track_id,
                '_force_cover_replace': bool(deezer_meta.get('cover_url')),
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
