"""
Download tracking functions for IDByRivoli.

Manages pending downloads, scheduled deletions, and
per-file download status for sequential mode.
"""
import os
import time
import shutil

from config import (
    track_download_status, track_download_status_lock,
    pending_downloads, pending_downloads_lock,
    scheduled_deletions, scheduled_deletions_lock,
    DELETION_DELAY_MINUTES, MAX_PENDING_TRACKS, PENDING_WARNING_THRESHOLD,
    OUTPUT_FOLDER, PROCESSED_FOLDER, SEQUENTIAL_MODE,
)


def _log_message(msg):
    """Fallback log function to avoid circular imports."""
    try:
        from services.queue_service import log_message
        log_message(msg)
    except ImportError:
        print(msg)


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
            _log_message(f"🗑️ Fichiers supprimés: {track_name}")
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
