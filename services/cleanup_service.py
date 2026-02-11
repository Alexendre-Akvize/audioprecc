"""
Cleanup service for IDByRivoli.

Disk monitoring, oldest-track deletion, delayed deletion, startup/periodic cleanup.
"""
import os
import shutil
import time
import threading
import psutil

from config import (
    PROCESSED_FOLDER,
    OUTPUT_FOLDER,
    UPLOAD_FOLDER,
    BASE_DIR,
    pending_downloads,
    pending_downloads_lock,
    track_download_status,
    track_download_status_lock,
    scheduled_deletions,
    scheduled_deletions_lock,
    disk_cleanup_in_progress,
    disk_cleanup_lock,
    DISK_THRESHOLD_PERCENT,
    TRACKS_TO_DELETE,
    DISK_CHECK_INTERVAL_SECONDS,
    DELAYED_DELETE_MINUTES,
    DELAYED_DELETE_ENABLED,
    CLEANUP_ON_START,
    MAX_FILE_AGE_HOURS,
    CLEANUP_INTERVAL_MINUTES,
)
import config


# ---------------------------------------------------------------------------
# Logging helper – avoids circular import with queue_service
# ---------------------------------------------------------------------------
def _log_message(message, session_id=None):
    """Fallback-safe wrapper around queue_service.log_message."""
    try:
        from services.queue_service import log_message
        log_message(message, session_id)
    except ImportError:
        print(message)


def _cleanup_track_after_downloads(track_name):
    """Wrapper for cleanup_track_after_downloads from utils.tracking."""
    try:
        from utils.tracking import cleanup_track_after_downloads
        cleanup_track_after_downloads(track_name)
    except (ImportError, AttributeError):
        pass


# =============================================================================
# DISK USAGE
# =============================================================================

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
    with disk_cleanup_lock:
        if config.disk_cleanup_in_progress:
            print("⚠️ Disk cleanup already in progress, skipping...")
            return 0
        config.disk_cleanup_in_progress = True
    
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
        
        _log_message(f"🗑️ Disk cleanup: {deleted_count} oldest tracks deleted, {freed_gb:.2f} GB freed")
        
    finally:
        with disk_cleanup_lock:
            config.disk_cleanup_in_progress = False
    
    return deleted_count


def cleanup_all_folders():
    """
    Aggressively clean ALL data folders (uploads, output, processed, htdemucs).
    Called when the processed folder is already empty but disk is still over threshold.
    Returns total bytes freed.
    """
    freed_bytes = 0
    deleted_count = 0

    folders_to_clean = [
        (UPLOAD_FOLDER, "uploads"),
        (OUTPUT_FOLDER, "output"),
        (PROCESSED_FOLDER, "processed"),
    ]

    for folder, name in folders_to_clean:
        if not os.path.exists(folder):
            continue
        for item in os.listdir(folder):
            item_path = os.path.join(folder, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    freed_bytes += os.path.getsize(item_path)
                    os.unlink(item_path)
                    deleted_count += 1
                elif os.path.isdir(item_path):
                    for dirpath, _dirnames, filenames in os.walk(item_path):
                        for f in filenames:
                            try:
                                freed_bytes += os.path.getsize(os.path.join(dirpath, f))
                            except Exception:
                                pass
                    shutil.rmtree(item_path)
                    deleted_count += 1
            except Exception as e:
                print(f"   ⚠️ Could not delete {item_path}: {e}")

    # Also clean covers folder
    covers_folder = os.path.join(BASE_DIR, 'static', 'covers')
    if os.path.exists(covers_folder):
        for filename in os.listdir(covers_folder):
            if filename.startswith('cover_'):
                try:
                    fp = os.path.join(covers_folder, filename)
                    freed_bytes += os.path.getsize(fp)
                    os.unlink(fp)
                    deleted_count += 1
                except Exception:
                    pass

    return freed_bytes, deleted_count


def disk_monitor_loop():
    """Background thread that monitors disk usage and triggers cleanup when needed."""
    # Cooldown: when cleanup finds nothing to delete, back off to avoid spamming
    _nothing_to_clean_until = 0  # timestamp until which we skip cleanup
    _COOLDOWN_SECONDS = 600      # 10 minutes cooldown when nothing left to clean
    _last_warning_logged = 0     # throttle repeated log messages

    while True:
        try:
            usage = get_disk_usage_percent()
            
            if usage >= DISK_THRESHOLD_PERCENT:
                now = time.time()

                # If we're in cooldown (nothing to delete), only log every 10 min
                if now < _nothing_to_clean_until:
                    if now - _last_warning_logged >= _COOLDOWN_SECONDS:
                        _log_message(
                            f"⚠️ Disk still at {usage:.1f}% but nothing left to clean "
                            f"— free space manually or raise DISK_THRESHOLD_PERCENT (currently {DISK_THRESHOLD_PERCENT}%)"
                        )
                        _last_warning_logged = now
                    time.sleep(DISK_CHECK_INTERVAL_SECONDS)
                    continue

                print(f"")
                print(f"⚠️ ════════════════════════════════════════════════")
                print(f"⚠️ DISK USAGE ALERT: {usage:.1f}% (threshold: {DISK_THRESHOLD_PERCENT}%)")
                print(f"⚠️ Starting cleanup of {TRACKS_TO_DELETE} oldest tracks...")
                print(f"⚠️ ════════════════════════════════════════════════")
                
                _log_message(f"⚠️ Disk usage {usage:.1f}% >= {DISK_THRESHOLD_PERCENT}% - Starting cleanup")
                
                deleted = delete_oldest_tracks(TRACKS_TO_DELETE)
                
                if deleted > 0:
                    new_usage = get_disk_usage_percent()
                    _log_message(f"✅ Cleanup complete: {deleted} tracks deleted, disk now at {new_usage:.1f}%")
                else:
                    # Processed folder was empty — try cleaning all folders aggressively
                    freed_bytes, extra_deleted = cleanup_all_folders()
                    new_usage = get_disk_usage_percent()

                    if extra_deleted > 0:
                        freed_gb = freed_bytes / (1024 * 1024 * 1024)
                        _log_message(
                            f"🗑️ Deep cleanup: {extra_deleted} leftover files removed, "
                            f"{freed_gb:.2f} GB freed, disk now at {new_usage:.1f}%"
                        )
                    else:
                        # Nothing at all to delete — enter cooldown
                        _nothing_to_clean_until = now + _COOLDOWN_SECONDS
                        _last_warning_logged = now
                        _log_message(
                            f"ℹ️ Disk at {usage:.1f}% but no local files to clean. "
                            f"Pausing cleanup checks for 10 min. "
                            f"Free space manually or set DISK_THRESHOLD_PERCENT > {DISK_THRESHOLD_PERCENT}"
                        )

                    # If disk is STILL over threshold after all cleanup, enter cooldown
                    if new_usage >= DISK_THRESHOLD_PERCENT and extra_deleted == 0:
                        _nothing_to_clean_until = now + _COOLDOWN_SECONDS
            else:
                # Disk is below threshold — reset cooldown
                _nothing_to_clean_until = 0
            
        except Exception as e:
            print(f"⚠️ Disk monitor error: {e}")
        
        time.sleep(DISK_CHECK_INTERVAL_SECONDS)


# =============================================================================
# DELAYED DELETION
# =============================================================================

def schedule_track_deletion_delayed(track_name, delay_minutes=None):
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
            _cleanup_track_after_downloads(track_name)
        except:
            pass
        
        try:
            with pending_downloads_lock:
                if track_name in pending_downloads:
                    del pending_downloads[track_name]
        except:
            pass
        
        _log_message(f"🗑️ Deleted '{track_name}' after {delay_minutes}min delay")
        
    except Exception as e:
        print(f"   ⚠️ Error deleting '{track_name}': {e}")
    
    finally:
        # Remove from scheduled deletions
        with scheduled_deletions_lock:
            if track_name in scheduled_deletions:
                del scheduled_deletions[track_name]


# =============================================================================
# STARTUP CLEANUP
# =============================================================================

def startup_cleanup():
    """
    Clears all storage on server startup to ensure clean state.
    This prevents disk from filling up between restarts.
    Can be disabled by setting CLEANUP_ON_START=false
    """
    if not CLEANUP_ON_START:
        _log_message("⏭️ Startup cleanup disabled (CLEANUP_ON_START=false)")
        return
    
    _log_message("🧹 ════════════════════════════════════════════════")
    _log_message("🧹 STARTUP CLEANUP: Clearing all storage...")
    
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
            _log_message(f"   🗑️ {name}: {file_count} items deleted ({size_mb:.1f} MB)")
    
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
            _log_message(f"   🗑️ covers: {cover_count} items deleted")
    
    # Clear pending downloads tracker
    with pending_downloads_lock:
        pending_downloads.clear()
    
    total_mb = total_size_freed / (1024 * 1024)
    total_gb = total_size_freed / (1024 * 1024 * 1024)
    
    if total_gb >= 1:
        _log_message(f"🧹 CLEANUP COMPLETE: {total_deleted} items, {total_gb:.2f} GB freed")
    else:
        _log_message(f"🧹 CLEANUP COMPLETE: {total_deleted} items, {total_mb:.1f} MB freed")
    _log_message("🧹 ════════════════════════════════════════════════")


# =============================================================================
# PERIODIC CLEANUP
# =============================================================================

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
