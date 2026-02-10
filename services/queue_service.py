"""
Queue service for IDByRivoli.

Session management, job status tracking, logging, and queue item tracking.
"""
import os
import time
import uuid

from flask import session as flask_session

from config import (
    sessions_status,
    sessions_lock,
    queue_items,
    queue_items_lock,
    MAX_PROCESSING_TIME,
    NUM_WORKERS,
)


# =============================================================================
# FILE DOWNLOAD LOGGING
# =============================================================================

def log_file_download(track_name, filepath):
    """
    Log when a file is downloaded (for monitoring purposes).
    Files are NOT deleted here - they stay until track.idbyrivoli.com confirms download.
    """
    file_basename = os.path.basename(filepath)
    print(f"📥 File downloaded for '{track_name}': {file_basename}")
    print(f"   ℹ️ File will remain available until download is confirmed via /confirm_download")


# =============================================================================
# SESSION MANAGEMENT
# =============================================================================

def get_session_id():
    """Get or create a unique session ID for the current user."""
    if 'session_id' not in flask_session:
        flask_session['session_id'] = str(uuid.uuid4())[:8]
    return flask_session['session_id']


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
    if session_id and session_id != 'global':
        status = get_job_status(session_id)
        status['logs'].append(f"[{timestamp}] {message}")
        if len(status['logs']) > 1000:
            status['logs'] = status['logs'][-1000:]
    
    # Also log to global for backward compatibility
    job_status['logs'].append(f"[{timestamp}] {message}")
    if len(job_status['logs']) > 1000:
        job_status['logs'] = job_status['logs'][-1000:]


# =============================================================================
# QUEUE ITEM TRACKING
# =============================================================================

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


# =============================================================================
# FAILED FILE TRACKING
# =============================================================================

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
