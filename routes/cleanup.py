"""
Cleanup routes blueprint for IDByRivoli.

Handles batch cleanup, oldest track deletion, batch counter reset,
scheduled deletions, and full file cleanup.
"""
import os
import shutil
import time

from flask import Blueprint, request, jsonify

import config
from config import (
    UPLOAD_FOLDER,
    OUTPUT_FOLDER,
    PROCESSED_FOLDER,
    BASE_DIR,
    track_queue,
    queue_items,
    queue_items_lock,
    pending_downloads,
    pending_downloads_lock,
    scheduled_deletions,
    scheduled_deletions_lock,
    DELAYED_DELETE_ENABLED,
    DELAYED_DELETE_MINUTES,
    DISK_THRESHOLD_PERCENT,
    DISK_CLEANUP_ENABLED,
    TRACKS_TO_DELETE,
    BATCH_SIZE,
    BATCH_MODE_ENABLED,
    batch_lock,
    track_download_status,
)
from services.queue_service import (
    log_message,
    job_status,
)
from services.cleanup_service import (
    get_disk_usage_percent,
    delete_oldest_tracks,
)
from utils.tracking import get_pending_tracks_count

cleanup_bp = Blueprint('cleanup', __name__)


@cleanup_bp.route('/batch_cleanup', methods=['POST'])
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


@cleanup_bp.route('/cleanup_oldest', methods=['POST'])
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


@cleanup_bp.route('/batch_reset', methods=['POST'])
def reset_batch_counter():
    """Reset the batch counter without cleanup."""
    with batch_lock:
        old_count = config.batch_processed_count
        config.batch_processed_count = 0
    
    log_message(f"🔄 Batch counter reset: {old_count} → 0")
    
    return jsonify({
        'success': True,
        'old_count': old_count,
        'new_count': 0
    })


@cleanup_bp.route('/scheduled_deletions')
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


@cleanup_bp.route('/cleanup', methods=['POST'])
def cleanup_files():
    """
    Deletes all files in uploads, output, and processed directories to free up disk space.
    Also clears all in-memory state to start fresh.
    """
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
        job_status['state'] = 'idle'
        job_status['progress'] = 0
        job_status['total_files'] = 0
        job_status['current_file_idx'] = 0
        job_status['current_filename'] = ''
        job_status['current_step'] = ''
        job_status['results'] = []  # IMPORTANT: Clear results
        job_status['error'] = None
        job_status['logs'] = []
        job_status['queue_size'] = 0
        
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
