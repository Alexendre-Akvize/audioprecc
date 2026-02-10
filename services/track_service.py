"""
Track processing service for IDByRivoli.

Demucs device detection, audio separation, edit generation, worker threads.
"""
import os
import subprocess
import threading
import time
import re
import gc
import fcntl
import urllib.parse

from pydub import AudioSegment
from mutagen.mp3 import MP3
from mutagen.id3 import ID3

import config
from config import (
    BASE_DIR,
    UPLOAD_FOLDER,
    OUTPUT_FOLDER,
    PROCESSED_FOLDER,
    DEMUCS_TIMEOUT_SECONDS,
    MEMORY_HIGH_THRESHOLD,
    MEMORY_CRITICAL_THRESHOLD,
    CPU_COUNT,
    NUM_WORKERS,
    track_queue,
    pending_downloads_lock,
    pending_downloads,
    track_download_status,
    track_download_status_lock,
    BATCH_SIZE,
    BATCH_MODE_ENABLED,
    batch_lock,
    MAX_RETRY_ATTEMPTS,
    RETRY_DELAY_SECONDS,
)
from services.memory_service import (
    get_memory_percent,
    force_garbage_collect,
    wait_for_memory_available,
)
from services.queue_service import (
    get_job_status,
    log_message,
    job_status,
    add_to_queue_tracker,
    update_queue_item,
    remove_from_queue_tracker,
    add_failed_file,
    remove_failed_file,
)
from services.metadata_service import (
    detect_track_type_from_title,
    extract_bpm_from_filename,
    strip_trailing_bpm_and_key,
    clean_detected_type_from_title,
    update_metadata,
    update_metadata_wav,
    prepare_track_metadata,
    send_track_info_to_api,
    search_deezer_metadata,
)
from utils.file_utils import clean_filename, format_artists, get_parent_label


# =============================================================================
# Lazy imports to avoid circular dependency
# =============================================================================
def _add_to_upload_history(filename, session_id, status='uploaded', track_type='Unknown', error=None):
    """Lazy import wrapper for add_to_upload_history from app.py (history module)."""
    try:
        from utils.history import add_to_upload_history
        add_to_upload_history(filename, session_id, status, track_type, error)
    except ImportError:
        print(f"⚠️ Could not import add_to_upload_history")


def _update_upload_history_status(filename, status, track_type=None, error=None):
    """Lazy import wrapper for update_upload_history_status."""
    try:
        from utils.history import update_upload_history_status
        update_upload_history_status(filename, status, track_type, error)
    except ImportError:
        print(f"⚠️ Could not import update_upload_history_status")


def _track_file_for_pending_download(track_name, original_path, num_files=6, file_list=None):
    """Wrapper for track_file_for_pending_download from utils.tracking."""
    try:
        from utils.tracking import track_file_for_pending_download
        track_file_for_pending_download(track_name, original_path, num_files, file_list)
    except (ImportError, AttributeError):
        print(f"⚠️ Could not import track_file_for_pending_download")


def _cleanup_track_after_downloads(track_name):
    """Wrapper for cleanup_track_after_downloads from utils.tracking."""
    try:
        from utils.tracking import cleanup_track_after_downloads
        cleanup_track_after_downloads(track_name)
    except (ImportError, AttributeError):
        pass


# =============================================================================
# GPU / DEVICE DETECTION
# =============================================================================

def get_demucs_device(force_check=False):
    """Detect best device for Demucs (CUDA GPU or CPU)."""
    
    # Allow override via environment variable
    force_device = config.FORCE_DEVICE
    if force_device in ('cuda', 'cpu'):
        print(f"🔧 Device forced via DEMUCS_FORCE_DEVICE={force_device}")
        return force_device
    
    try:
        import torch
        
        print(f"🔍 GPU Detection:")
        print(f"   PyTorch version: {torch.__version__}")
        print(f"   CUDA compiled: {torch.version.cuda or 'NO'}")
        print(f"   CUDA available: {torch.cuda.is_available()}")
        
        if hasattr(torch.backends, 'cuda'):
            print(f"   CUDA backend built: {torch.backends.cuda.is_built()}")
        if hasattr(torch.backends, 'cudnn'):
            print(f"   cuDNN available: {torch.backends.cudnn.is_available()}")
            print(f"   cuDNN version: {torch.backends.cudnn.version() if torch.backends.cudnn.is_available() else 'N/A'}")
        
        # Check NVIDIA driver
        try:
            nvidia_result = subprocess.run(['nvidia-smi', '--query-gpu=name,memory.total,driver_version', '--format=csv,noheader'], 
                                          capture_output=True, text=True, timeout=10)
            if nvidia_result.returncode == 0:
                print(f"   nvidia-smi: {nvidia_result.stdout.strip()}")
            else:
                print(f"   nvidia-smi: FAILED (code {nvidia_result.returncode})")
        except Exception as e:
            print(f"   nvidia-smi: NOT FOUND ({e})")
        
        # Force CUDA initialization
        if torch.cuda.is_available():
            try:
                torch.cuda.init()
                device_count = torch.cuda.device_count()
                if device_count > 0:
                    gpu_name = torch.cuda.get_device_name(0)
                    gpu_mem = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                    
                    # Verify CUDA actually works by running a small tensor operation
                    test_tensor = torch.zeros(1, device='cuda')
                    del test_tensor
                    
                    print(f"🚀 GPU détecté: {gpu_name} ({gpu_mem:.0f}GB) - Mode CUDA activé")
                    print(f"   CUDA version: {torch.version.cuda}")
                    print(f"   ✅ CUDA test tensor: PASSED")
                    return 'cuda'
                else:
                    print(f"   ⚠️ torch.cuda.device_count() = 0")
            except Exception as e:
                print(f"⚠️ CUDA disponible mais erreur d'init: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"⚠️ torch.cuda.is_available() = False")
            print(f"   This usually means PyTorch was installed WITHOUT CUDA support.")
            print(f"   Fix: pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121")
    except ImportError:
        print(f"❌ PyTorch not installed! Cannot use GPU.")
    except Exception as e:
        print(f"❌ Erreur détection GPU: {e}")
        import traceback
        traceback.print_exc()
    
    print("💻 Mode CPU activé (⚠️ MUCH SLOWER - GPU recommended)")
    return 'cpu'


def ensure_cuda_device():
    """Re-check CUDA availability (call before processing)."""
    if config.DEMUCS_DEVICE == 'cpu' and not config.FORCE_DEVICE:
        # Try again in case CUDA wasn't ready at import time
        new_device = get_demucs_device(force_check=True)
        if new_device == 'cuda':
            config.DEMUCS_DEVICE = new_device
            print(f"🚀 CUDA now available! Switching from CPU to GPU")
    return config.DEMUCS_DEVICE


# =============================================================================
# EDIT GENERATION
# =============================================================================

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
    
    # Fallback: extract BPM from filename trailing number (DJ City format)
    if bpm is None:
        bpm = extract_bpm_from_filename(base_filename)
        if bpm:
            log_message(f"BPM depuis nom de fichier: {bpm}")
    
    if bpm is None:
        log_message(f"⚠️ Pas de BPM dans les métadonnées originales ni le nom de fichier")
    
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
    
    # Remove trailing BPM and/or Camelot key from base name (e.g., "10A 93", "1B", "102")
    metadata_base_name = strip_trailing_bpm_and_key(metadata_base_name)
    
    # Create correct output directory using metadata title
    correct_output_path = os.path.join(PROCESSED_FOLDER, metadata_base_name)
    os.makedirs(correct_output_path, exist_ok=True)
    
    edits = []

    def export_edit(audio_segment, suffix):
        # Use metadata_base_name computed above
        base_name = metadata_base_name
        
        out_name_mp3 = f"{base_name} - {suffix}.mp3"
        out_name_wav = f"{base_name} - {suffix}.wav"
        
        # Use correct_output_path (based on metadata title)
        out_path_mp3 = os.path.join(correct_output_path, out_name_mp3)
        out_path_wav = os.path.join(correct_output_path, out_name_wav)
        
        # Metadata title uses the same base name + suffix
        metadata_title = f"{base_name} - {suffix}"
        
        # Export sequentially to avoid thread nesting (this is already called from a thread pool)
        # This prevents thread explosion that was causing OOM crashes
        audio_segment.export(out_path_mp3, format="mp3", bitrate="320k")
        update_metadata(out_path_mp3, "ID By Rivoli", metadata_title, original_path, bpm)
        
        audio_segment.export(out_path_wav, format="wav")
        update_metadata_wav(out_path_wav, "ID By Rivoli", metadata_title, original_path, bpm)
        
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
        base_url = config.CURRENT_HOST_URL if config.CURRENT_HOST_URL else "http://localhost:8888"
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
            del vocals_audio  # Free memory immediately after analysis
            
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
    del original  # Free memory immediately
    
    # 2. Acapella (Vocals only) - Only if vocals detected
    if vocals_path and os.path.exists(vocals_path) and vocals_detected:
        vocals = AudioSegment.from_mp3(vocals_path)
        edits.append(export_edit(vocals, "Acapella"))
        del vocals  # Free memory immediately
        log_message(f"✓ Version Acapella créée")
    elif vocals_path and os.path.exists(vocals_path) and not vocals_detected:
        log_message(f"⏭️ Acapella ignorée (pas de voix détectées)")
    else:
        log_message(f"⚠️ Pas de fichier vocals pour Acapella")
    
    # 3. Instrumental (No vocals) - Always if available
    if inst_path and os.path.exists(inst_path):
        instrumental = AudioSegment.from_mp3(inst_path)
        edits.append(export_edit(instrumental, "Instrumental"))
        del instrumental  # Free memory immediately
        log_message(f"✓ Version Instrumentale créée")
    else:
        log_message(f"⚠️ Pas de fichier instrumental")
    
    # Force garbage collection after processing all edits (frees large audio buffers)
    gc.collect()
    
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
    
    _track_file_for_pending_download(metadata_base_name, original_path, num_files, file_list)

    return edits


def run_demucs_thread(filepaths, original_filenames):
    try:
        job_status['state'] = 'processing'
        job_status['total_files'] = len(filepaths)
        job_status['results'] = []
        job_status['progress'] = 0

        current_file_index = 0

        # Dynamic chunk size: reduce if memory is high
        BATCH_CHUNK_SIZE = 50
        for i in range(0, len(filepaths), BATCH_CHUNK_SIZE):
            # Check memory before each chunk
            mem = get_memory_percent()
            if mem >= MEMORY_HIGH_THRESHOLD:
                print(f"⚠️ Batch: RAM at {mem:.1f}% - pausing before next chunk...")
                force_garbage_collect("Batch pre-chunk")
                wait_for_memory_available(worker_id=0, timeout=120)
            
            chunk = filepaths[i:i + BATCH_CHUNK_SIZE]
            
            # Optimized Demucs settings for batch processing (H100 + 240GB RAM)
            # Re-check CUDA before batch (may have become available after startup)
            ensure_cuda_device()
            
            # CRITICAL: -j controls CPU threads PER Demucs process
            # Batch mode runs chunks sequentially (1 Demucs process at a time)
            # so we can use more jobs here than in parallel worker mode
            if config.DEMUCS_DEVICE == 'cuda':
                # In batch mode, only 1 Demucs process runs at a time
                # So we can safely use more CPU threads
                batch_jobs = max(2, CPU_COUNT // 2)
            else:
                batch_jobs = max(1, CPU_COUNT // 2)
                log_message(f"⚠️ Batch Demucs on CPU with {batch_jobs} job(s) - GPU recommended!")
            
            # Use demucs_runner.py wrapper to fix torchaudio/torchcodec compatibility
            DEMUCS_RUNNER = os.path.join(BASE_DIR, 'demucs_runner.py')
            command = [
                'python3', DEMUCS_RUNNER,
                '--two-stems=vocals',
                '-n', 'htdemucs',
                '--mp3',
                '--mp3-bitrate', '320',
                '-j', str(batch_jobs),        # Maximum parallelism
                '--segment', '7',             # Max segment size (integer)
                '--overlap', '0.1',           # Minimal for speed
                '--device', config.DEMUCS_DEVICE,    # GPU/CPU auto-detection
                '-o', OUTPUT_FOLDER
            ] + chunk

            chunk_num = i // 50 + 1
            total_chunks = (len(filepaths) - 1) // 50 + 1
            log_message(f"Démarrage de la séparation IA (Lot {chunk_num}/{total_chunks})...")
            
            process = subprocess.Popen(
                command, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True, 
                bufsize=1, 
                universal_newlines=True
            )

            current_chunk_base = i
            last_output_time = time.time()

            for line in process.stdout:
                last_output_time = time.time()
                print(line, end='')
                
                if "Separating track" in line:
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
                    
                    percent_per_file = 50 / len(filepaths)
                    base_progress = (current_file_index - 1) * percent_per_file
                    job_status['progress'] = int(base_progress)
                    job_status['current_step'] = f"Séparation IA (Lot {chunk_num}/{total_chunks})"

                elif "%|" in line:
                    try:
                        parts = line.split('%|')
                        if len(parts) > 0:
                            percent_part = parts[0].strip()
                            p_match = re.search(r'(\d+)$', percent_part)
                            if p_match:
                                track_percent = int(p_match.group(1))
                                
                                percent_per_file = 50 / len(filepaths)
                                base_progress = (current_file_index - 1) * percent_per_file
                                added_progress = (track_percent / 100) * percent_per_file
                                job_status['progress'] = int(base_progress + added_progress)
                    except:
                        pass
            
            process.wait()
            
            # Force GC between chunks to free memory
            force_garbage_collect(f"Batch chunk {chunk_num}/{total_chunks}")
            
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
        # Limit workers to avoid memory overload (each edit loads ~3 audio files)
        edit_workers = max(2, min(NUM_WORKERS // 2, CPU_COUNT // 2, 8))
        print(f"🚀 Génération des edits avec {edit_workers} workers parallèles [RAM: {get_memory_percent():.1f}%]")
        
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


# =============================================================================
# PROCESS TRACK WITHOUT SEPARATION
# =============================================================================

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
        
        # Remove trailing BPM and/or Camelot key from base name (e.g., "10A 93", "1B", "102")
        metadata_base_name = strip_trailing_bpm_and_key(metadata_base_name)
        
        # Clean type markers from base name to avoid redundancy
        # e.g., "Tt Freak (Clean)" with type "Original Clean" → base "Tt Freak"
        # e.g., "Trompeta Y Fiesta (Djcity Intro) 130" → "Trompeta Y Fiesta"
        cleaned_base = clean_detected_type_from_title(metadata_base_name, track_type)
        if cleaned_base:
            metadata_base_name = cleaned_base
            log_message(f"📝 Cleaned base title: '{metadata_base_name}' (from type markers)")
        
        # Get BPM from original metadata
        bpm = None
        try:
            if original_tags and 'TBPM' in original_tags:
                bpm_text = str(original_tags['TBPM'].text[0]).strip()
                if bpm_text:
                    bpm = int(float(bpm_text))
        except:
            pass
        
        # Fallback: extract BPM from filename trailing number (DJ City format)
        if bpm is None:
            bpm = extract_bpm_from_filename(filename)
            if bpm:
                log_message(f"BPM depuis nom de fichier: {bpm}")
        
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
        
        # Export both formats sequentially (avoids thread nesting & memory spikes)
        original.export(out_path_mp3, format="mp3", bitrate="320k")
        update_metadata(out_path_mp3, "ID By Rivoli", metadata_title, filepath, bpm)
        
        original.export(out_path_wav, format="wav")
        update_metadata_wav(out_path_wav, "ID By Rivoli", metadata_title, filepath, bpm)
        
        del original  # Free audio memory immediately
        gc.collect()
        
        update_queue_item(filename, progress=80, step='Envoi API...')
        
        # Build URLs
        rel_path_mp3 = f"{metadata_base_name}/{out_name_mp3}"
        rel_path_wav = f"{metadata_base_name}/{out_name_wav}"
        
        mp3_url = f"/download_file?path={urllib.parse.quote(rel_path_mp3, safe='/')}"
        wav_url = f"/download_file?path={urllib.parse.quote(rel_path_wav, safe='/')}"
        
        # Log URLs
        base_url = config.CURRENT_HOST_URL if config.CURRENT_HOST_URL else "http://localhost:8888"
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
        _track_file_for_pending_download(metadata_base_name, filepath, 2, file_list)
        
        update_queue_item(filename, progress=100, step=f'Terminé ({track_type}) ✅')
        
        # Add to session results
        current_status['results'].append({
            'original': metadata_base_name,
            'edits': [edit]
        })
        
        # Update upload history
        _update_upload_history_status(filename, 'completed', track_type=track_type)
        
        log_message(f"✅ [{session_id}] Export direct terminé: {metadata_base_name} ({track_type})", session_id)
        
        current_status['progress'] = 100
        return True, None
        
    except Exception as e:
        error_msg = f"Erreur export direct: {str(e)}"
        log_message(f"❌ {error_msg} pour {filename}", session_id)
        import traceback
        traceback.print_exc()
        _update_upload_history_status(filename, 'failed', error=error_msg)
        return False, error_msg


# =============================================================================
# PROCESS SINGLE TRACK (with retry logic)
# =============================================================================

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
            _add_to_upload_history(filename, session_id, 'processing', detected_type)
            
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
                
                if device == 'cuda':
                    jobs = max(1, CPU_COUNT // max(NUM_WORKERS, 1))
                    log_message(f"🚀 GPU mode: {jobs} job(s) per worker × {NUM_WORKERS} workers = {jobs * NUM_WORKERS} total CPU threads (CPUs: {CPU_COUNT})")
                else:
                    jobs = max(1, CPU_COUNT // max(NUM_WORKERS * 2, 1))
                    log_message(f"⚠️ CPU mode: {jobs} job(s) per worker × {NUM_WORKERS} workers")
                
                # Use demucs_runner.py wrapper to fix torchaudio/torchcodec compatibility
                DEMUCS_RUNNER = os.path.join(BASE_DIR, 'demucs_runner.py')
                cmd = [
                    'python3', DEMUCS_RUNNER,
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
                last_output_time = time.time()
                
                # Make stdout non-blocking for timeout support
                fd = proc.stdout.fileno()
                fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
                
                while proc.poll() is None:
                    # Check for timeout (no output for DEMUCS_TIMEOUT_SECONDS)
                    elapsed_since_output = time.time() - last_output_time
                    if elapsed_since_output > DEMUCS_TIMEOUT_SECONDS:
                        print(f"🔴 DEMUCS TIMEOUT: No output for {DEMUCS_TIMEOUT_SECONDS}s - killing process {proc.pid}")
                        proc.kill()
                        proc.wait()
                        output_lines.append(f"TIMEOUT: Process killed after {DEMUCS_TIMEOUT_SECONDS}s of no output\n")
                        return -1, output_lines
                    
                    # Check memory during Demucs processing
                    mem = get_memory_percent()
                    if mem >= MEMORY_CRITICAL_THRESHOLD:
                        print(f"🔴 MEMORY CRITICAL during Demucs: {mem:.1f}% - killing process {proc.pid}")
                        proc.kill()
                        proc.wait()
                        force_garbage_collect("Demucs killed due to memory")
                        output_lines.append(f"OOM: Process killed, RAM at {mem:.1f}%\n")
                        return -2, output_lines
                    
                    # Try to read available output
                    try:
                        line = proc.stdout.readline()
                        if line:
                            last_output_time = time.time()
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
                                            update_queue_item(filename, progress=int(track_percent * 0.7), step=f'Séparation IA {track_percent}%{retry_label}')
                                except:
                                    pass
                        else:
                            time.sleep(0.5)  # Brief sleep when no output available
                    except (IOError, OSError):
                        time.sleep(0.5)  # No data available yet
                
                # Read any remaining output after process ends
                try:
                    remaining = proc.stdout.read()
                    if remaining:
                        for line in remaining.splitlines(True):
                            print(line, end='')
                            output_lines.append(line)
                except:
                    pass
                
                return proc.returncode, output_lines
            
            # Try with detected device first
            returncode, demucs_output = run_demucs_with_device(config.DEMUCS_DEVICE)
            
            # If GPU failed, fallback to CPU
            if returncode != 0 and config.DEMUCS_DEVICE == 'cuda':
                log_message(f"⚠️ GPU échoué, fallback vers CPU...")
                returncode, demucs_output = run_demucs_with_device('cpu')
            
            if returncode != 0:
                error_lines = ''.join(demucs_output[-20:])
                # Show a longer snippet in logs so the real error is visible
                error_msg = f"Erreur Demucs (code {returncode}): {error_lines[:500]}"
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
            _update_upload_history_status(filename, 'completed', track_type='Full Analysis')
            
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
    _update_upload_history_status(filename, 'failed', error=final_error)
    
    # Update UI to show failure
    update_queue_item(filename, status='failed', progress=0, step=f'❌ Échec: {last_error[:50]}...')
    
    return False, final_error


# =============================================================================
# GIT INFO
# =============================================================================

def get_git_info():
    try:
        # Get hash
        hash_output = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).strip().decode('utf-8')
        # Get date
        date_output = subprocess.check_output(['git', 'log', '-1', '--format=%cd', '--date=format:%a %b %d %H:%M']).strip().decode('utf-8')
        
        count = subprocess.check_output(['git', 'rev-list', '--count', 'HEAD']).strip().decode('utf-8')
        
        return f"v0.{count} ({hash_output}) - {date_output}"
    except:
        return "Dev Version"


# =============================================================================
# OPTIMAL WORKERS
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
            
            if gpu_mem_gb >= 70 and ram_gb >= 200:  # H100 + High RAM
                num_workers = 16
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
    
    # CPU fallback
    cpu_workers = max(1, min(3, CPU_COUNT // 6))
    print(f"⚠️ CPU MODE: Only {cpu_workers} workers (CPU can't parallelize like GPU)")
    print(f"   💡 To use GPU: ensure PyTorch is installed with CUDA support")
    print(f"   💡 Or set DEMUCS_FORCE_DEVICE=cuda in .env if GPU is available")
    return cpu_workers


# =============================================================================
# BATCH TRACKING
# =============================================================================

def increment_batch_count():
    """Increment the batch counter for tracking purposes (no pause, no delete)."""
    if not BATCH_MODE_ENABLED:
        return
    
    with batch_lock:
        config.batch_processed_count += 1
        count = config.batch_processed_count
        
        # Log progress every BATCH_SIZE tracks
        if count % BATCH_SIZE == 0:
            print(f"📊 Milestone: {count} tracks processed (continuous processing, no pause)")
            log_message(f"📊 {count} tracks traités (traitement continu)")


def wait_for_batch_resume():
    """Legacy function - no longer pauses, kept for compatibility."""
    pass  # No pause - continuous processing


# =============================================================================
# WORKER THREAD
# =============================================================================

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
            
            # MEMORY SAFETY: Wait if RAM is too high before starting new track
            wait_for_memory_available(worker_id)
            
            print(f"🔄 Worker {worker_id} traite: {filename}" + (" (RETRY)" if is_retry else "") + f" [RAM: {get_memory_percent():.1f}%]")
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
            
            # MEMORY SAFETY: Force garbage collection after each track
            force_garbage_collect(f"Worker {worker_id} after {filename}")
            
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
