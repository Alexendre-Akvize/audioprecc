"""
Status routes blueprint for IDByRivoli.

Handles system status, live logs, queue debug, database status, and debug routes.
"""
import os
import subprocess
import time
import urllib.parse

import psutil
from flask import Blueprint, request, jsonify

import config
from config import (
    PROCESSED_FOLDER,
    OUTPUT_FOLDER,
    CPU_COUNT,
    track_queue,
    queue_items,
    queue_items_lock,
    worker_threads,
    MAX_PROCESSING_TIME,
    USE_DATABASE_MODE,
    API_ENDPOINT,
    DELETION_DELAY_MINUTES,
    MAX_FILE_AGE_HOURS,
    CLEANUP_INTERVAL_MINUTES,
    DELETE_AFTER_DOWNLOAD,
    MAX_PENDING_TRACKS,
    PENDING_WARNING_THRESHOLD,
    scheduled_deletions,
    scheduled_deletions_lock,
    track_download_status,
    track_download_status_lock,
    pending_downloads,
    pending_downloads_lock,
)
from services.queue_service import (
    get_session_id,
    get_job_status,
    log_message,
    job_status,
    add_to_queue_tracker,
    remove_from_queue_tracker,
    get_queue_items_list,
    remove_failed_file,
)
from services.memory_service import get_memory_percent
from utils.tracking import (
    get_pending_tracks_count,
    check_pending_tracks_warning,
    get_pending_tracks_list,
)

status_bp = Blueprint('status', __name__)


@status_bp.route('/api/live_logs')
def live_logs():
    """Returns recent log lines for the live logs panel in the UI."""
    session_id = request.args.get('session_id') or get_session_id()
    since_index = request.args.get('since', 0, type=int)
    limit = request.args.get('limit', 100, type=int)
    
    current_status = get_job_status(session_id)
    all_logs = current_status.get('logs', [])
    
    # Also include global logs for system-wide messages
    global_logs = job_status.get('logs', [])
    
    # Merge and deduplicate (global logs may overlap with session logs)
    # Use global logs as the comprehensive source
    logs = global_logs
    
    total = len(logs)
    
    # If since_index is provided, return only new logs since that index
    if since_index > 0 and since_index < total:
        new_logs = logs[since_index:]
    else:
        new_logs = logs[-limit:]
    
    # Add memory/CPU context to response
    mem_percent = get_memory_percent()
    cpu_percent = psutil.cpu_percent(interval=0)
    
    return jsonify({
        'logs': new_logs,
        'total': total,
        'since': since_index,
        'mem_percent': round(mem_percent, 1),
        'cpu_percent': round(cpu_percent, 1),
        'queue_size': track_queue.qsize(),
        'active_workers': sum(1 for t in worker_threads if t.is_alive()),
    })


@status_bp.route('/status')
def status():
    # Get session-specific status
    session_id = request.args.get('session_id') or get_session_id()
    current_status = get_job_status(session_id)
    
    # Update queue info
    current_status['queue_size'] = track_queue.qsize()
    current_status['num_workers'] = config.NUM_WORKERS
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


@status_bp.route('/retry_failed', methods=['POST'])
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


@status_bp.route('/clear_failed', methods=['POST'])
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


@status_bp.route('/failed_files')
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


@status_bp.route('/reset_stuck_items', methods=['POST'])
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


@status_bp.route('/queue_debug')
def queue_debug():
    """
    Debug endpoint to inspect the current state of the queue and workers.
    """
    with queue_items_lock:
        items_by_status = {}
        for filename, info in queue_items.items():
            _status = info['status']
            if _status not in items_by_status:
                items_by_status[_status] = []
            items_by_status[_status].append({
                'filename': filename,
                'worker': info.get('worker'),
                'progress': info.get('progress'),
                'processing_started_at': info.get('processing_started_at'),
                'time_processing': round(time.time() - info.get('processing_started_at', time.time())) if info.get('processing_started_at') else None
            })
    
    return jsonify({
        'total_items': len(queue_items),
        'queue_size': track_queue.qsize(),
        'num_workers': config.NUM_WORKERS,
        'active_workers': sum(1 for t in worker_threads if t.is_alive()),
        'items_by_status': items_by_status,
        'max_processing_time_seconds': MAX_PROCESSING_TIME
    })


@status_bp.route('/system_stats')
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
            'device': config.DEMUCS_DEVICE,
            'num_workers': config.NUM_WORKERS,
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
    
    # GPU stats - use nvidia-smi for real utilization (subprocess GPU usage)
    try:
        import subprocess as _sp
        nvidia_result = _sp.run(
            ['nvidia-smi', '--query-gpu=name,memory.total,memory.used,utilization.gpu',
             '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=5
        )
        if nvidia_result.returncode == 0:
            parts = [p.strip() for p in nvidia_result.stdout.strip().split(',')]
            if len(parts) >= 4:
                stats['gpu']['available'] = True
                stats['gpu']['name'] = parts[0]
                total_mb = float(parts[1])
                used_mb = float(parts[2])
                stats['gpu']['memory_gb'] = round(total_mb / 1024, 1)
                stats['gpu']['memory_used_gb'] = round(used_mb / 1024, 2)
                stats['gpu']['memory_used_percent'] = round(used_mb / total_mb * 100, 1) if total_mb > 0 else 0
                stats['gpu']['utilization_percent'] = int(parts[3])
    except Exception:
        # Fallback to PyTorch stats (only sees parent process allocations)
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
        except Exception:
            pass
    
    return jsonify(stats)


# =============================================================================
# DATABASE MODE STATUS
# =============================================================================

@status_bp.route('/database_status')
def database_status():
    """Get the current database mode status and connection info."""
    status_data = {
        'database_mode_enabled': USE_DATABASE_MODE,
        'api_endpoint': API_ENDPOINT if not USE_DATABASE_MODE else None,
    }
    
    if USE_DATABASE_MODE:
        try:
            from database_service import check_database_connection, get_database_service, get_schema_info, test_database_insert
            db = get_database_service()
            connected = check_database_connection()
            status_data['database_connected'] = connected
            status_data['database_host'] = os.environ.get('DATABASE_HOST', 'from DATABASE_URL')
            status_data['database_name'] = os.environ.get('DATABASE_NAME', 'from DATABASE_URL')
            status_data['database_url_set'] = bool(os.environ.get('DATABASE_URL'))
            
            if connected:
                # Get schema info
                schema_info = get_schema_info()
                status_data['schema'] = {
                    'track_table_exists': schema_info.get('track_table_exists', False),
                    'tables_count': len(schema_info.get('tables', [])),
                    'track_columns_count': len(schema_info.get('track_columns', [])),
                }
                
                # Show some track columns for debugging
                track_cols = schema_info.get('track_columns', [])
                status_data['schema']['sample_columns'] = [c['column_name'] for c in track_cols[:20]]
                
                # Test insert capability
                test_result = test_database_insert()
                status_data['insert_test'] = test_result
                
        except Exception as e:
            status_data['database_connected'] = False
            status_data['database_error'] = str(e)
            import traceback
            status_data['traceback'] = traceback.format_exc()
    else:
        status_data['database_connected'] = False
        status_data['note'] = 'Database mode disabled - using external API'
    
    return jsonify(status_data)


# =============================================================================
# BATCH STATUS
# =============================================================================

@status_bp.route('/batch_status')
def get_batch_status():
    """Get the current batch processing status including disk info."""
    from services.cleanup_service import get_disk_usage_percent
    from config import (
        BATCH_MODE_ENABLED, BATCH_SIZE, batch_processed_count, batch_lock,
        DISK_THRESHOLD_PERCENT, DISK_CLEANUP_ENABLED, TRACKS_TO_DELETE,
        disk_cleanup_in_progress,
    )
    
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


# =============================================================================
# DEBUG ROUTES
# =============================================================================

@status_bp.route('/debug_url')
def debug_url():
    """Debug route to see the detected public URL and request headers."""
    return jsonify({
        'CURRENT_HOST_URL': config.CURRENT_HOST_URL,
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


@status_bp.route('/debug_cleanup')
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


@status_bp.route('/debug_gpu')
def debug_gpu():
    """Debug route to check GPU/CUDA status."""
    info = {
        'demucs_device': config.DEMUCS_DEVICE,
        'force_device_env': config.FORCE_DEVICE or 'auto',
        'num_workers': config.NUM_WORKERS,
        'cpu_count': CPU_COUNT,
        'cuda_available': False,
        'cuda_version': None,
        'pytorch_version': None,
        'gpu_name': None,
        'gpu_memory_gb': None,
        'gpu_count': 0,
        'ram_gb': round(psutil.virtual_memory().total / (1024**3), 1),
        'ram_used_percent': psutil.virtual_memory().percent,
        'cpu_percent': psutil.cpu_percent(interval=0.5),
        'nvidia_smi': None,
        'fix_suggestions': [],
        'error': None
    }
    
    # Check nvidia-smi
    try:
        nvidia_result = subprocess.run(['nvidia-smi'], capture_output=True, text=True, timeout=10)
        info['nvidia_smi'] = nvidia_result.stdout if nvidia_result.returncode == 0 else f"FAILED: {nvidia_result.stderr}"
    except Exception as e:
        info['nvidia_smi'] = f"NOT FOUND: {e}"
    
    try:
        import torch
        info['pytorch_version'] = torch.__version__
        info['cuda_available'] = torch.cuda.is_available()
        info['cuda_compiled_version'] = torch.version.cuda or 'NO CUDA IN THIS BUILD'
        
        if hasattr(torch.backends, 'cuda'):
            info['cuda_backend_built'] = torch.backends.cuda.is_built()
        if hasattr(torch.backends, 'cudnn'):
            info['cudnn_available'] = torch.backends.cudnn.is_available()
        
        if not torch.cuda.is_available():
            info['fix_suggestions'].append('PyTorch has NO CUDA support. Reinstall with: pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121')
            if not torch.version.cuda:
                info['fix_suggestions'].append('This PyTorch build was compiled WITHOUT CUDA (CPU-only version)')
        
        if torch.cuda.is_available():
            info['gpu_count'] = torch.cuda.device_count()
            if info['gpu_count'] > 0:
                info['gpu_name'] = torch.cuda.get_device_name(0)
                props = torch.cuda.get_device_properties(0)
                info['gpu_memory_gb'] = round(props.total_memory / (1024**3), 1)
                info['gpu_memory_allocated_gb'] = round(torch.cuda.memory_allocated(0) / (1024**3), 2)
                info['gpu_memory_reserved_gb'] = round(torch.cuda.memory_reserved(0) / (1024**3), 2)
                
                if config.DEMUCS_DEVICE == 'cpu':
                    info['fix_suggestions'].append('GPU is available but Demucs is using CPU! Try restarting the app or set DEMUCS_FORCE_DEVICE=cuda in .env')
    except ImportError:
        info['error'] = 'PyTorch not installed'
        info['fix_suggestions'].append('Install PyTorch with CUDA: pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121')
    except Exception as e:
        info['error'] = str(e)
    
    if config.DEMUCS_DEVICE == 'cpu':
        info['fix_suggestions'].append('Workaround: Add DEMUCS_FORCE_DEVICE=cuda to your .env file and restart')
    
    return jsonify(info)


@status_bp.route('/test_download')
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
