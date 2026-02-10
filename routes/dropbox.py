"""
Dropbox routes blueprint for IDByRivoli.

Handles Dropbox file browsing, importing, and bulk import operations.
"""
import os
import re
import json
import time
import uuid
import threading
from threading import Lock

import requests
from flask import Blueprint, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv
import psutil

import config
from config import (
    UPLOAD_FOLDER,
    PROCESSED_FOLDER,
    OUTPUT_FOLDER,
    DROPBOX_TEAM_MEMBER_ID,
    track_queue,
    queue_items,
    queue_items_lock,
    dropbox_imports,
    dropbox_imports_lock,
    bulk_import_state,
    bulk_import_lock,
    MEMORY_HIGH_THRESHOLD,
    MEMORY_CRITICAL_THRESHOLD,
)
from services.dropbox_service import get_valid_dropbox_token, is_token_expired_error
from services.queue_service import (
    get_session_id,
    log_message,
    add_to_queue_tracker,
)
from services.memory_service import get_memory_percent, force_garbage_collect
from services.metadata_service import process_track_title_for_import, delete_from_dropbox_if_skipped
from utils.file_utils import is_track_already_processed

dropbox_bp = Blueprint('dropbox', __name__)


@dropbox_bp.route('/dropbox/list', methods=['POST'])
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
    
    # Get valid token (auto-refreshes if expired)
    dropbox_token = get_valid_dropbox_token()
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    print(f"📦 Dropbox list request - Token configured: {bool(dropbox_token)}, Token length: {len(dropbox_token) if dropbox_token else 0}, Team member ID: {bool(dropbox_team_member_id)}")
    
    if not dropbox_token:
        return jsonify({'error': 'Dropbox not configured. Set DROPBOX_ACCESS_TOKEN and DROPBOX_REFRESH_TOKEN in .env'}), 400
    
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


@dropbox_bp.route('/dropbox/scan_all', methods=['GET'])
def dropbox_scan_all_files():
    """
    Recursively scan entire Dropbox (or a folder) for all MP3/WAV files.
    Uses Server-Sent Events to stream results in real-time.
    
    Query params:
    - folder_path: Optional starting folder (empty for entire Dropbox)
    
    Returns SSE stream with files as they are found.
    """
    load_dotenv(override=True)
    
    # Get valid token (auto-refreshes if expired)
    dropbox_token = get_valid_dropbox_token()
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

@dropbox_bp.route('/dropbox/bulk_import/start', methods=['POST'])
def start_bulk_import():
    """
    Start a bulk import from Dropbox. Scans recursively and processes each track.
    Runs in background - continues even if browser closes.
    """
    with bulk_import_lock:
        if bulk_import_state['active']:
            return jsonify({
                'error': 'A bulk import is already running',
                'status': bulk_import_state['current_status']
            }), 400
    
    load_dotenv(override=True)
    
    # Get valid token (auto-refreshes if expired)
    dropbox_token = get_valid_dropbox_token()
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    if not dropbox_token:
        return jsonify({'error': 'Dropbox not configured. Set DROPBOX_REFRESH_TOKEN in .env'}), 400
    
    data = request.json or {}
    folder_path = data.get('folder_path', '').strip()
    
    # Normalize path
    if folder_path and not folder_path.startswith('/'):
        folder_path = '/' + folder_path
    if folder_path == '/':
        folder_path = ''
    
    # Reset state
    with bulk_import_lock:
        bulk_import_state.update({
            'active': True,
            'stop_requested': False,
            'folder_path': folder_path,
            'namespace_id': '',
            'started_at': time.time(),
            'total_found': 0,
            'scanning_found': 0,
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
        })
    
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


@dropbox_bp.route('/dropbox/bulk_import/status')
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
            'scanning_found': bulk_import_state.get('scanning_found', 0),
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


@dropbox_bp.route('/dropbox/bulk_import/stop', methods=['POST'])
def stop_bulk_import():
    """Request to stop the bulk import."""
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
    CONTINUOUS SMART PIPELINE: Runs forever until manually stopped.
    
    Strategy:
    1. Scan Dropbox folder for files
    2. Download tracks in parallel (fill buffer up to BUFFER_SIZE)
    3. Workers process continuously from queue
    4. Move to /track done/ in Dropbox when processing succeeds
    5. When all done, wait and scan again for new files
    6. ONLY stops when manually interrupted or after consecutive empty scans
    
    This maximizes throughput by keeping workers always busy.
    """
    # Configuration - scale buffer based on worker count to avoid overwhelming resources
    # Each queued track takes ~5-15MB disk + the worker uses ~2-4GB RAM for demucs
    # Keep buffer proportional to what workers can actually process
    BUFFER_SIZE = max(5, config.NUM_WORKERS * 5)  # 5 tracks per worker (not 200 hardcoded)
    DOWNLOAD_BATCH = max(2, config.NUM_WORKERS * 2)  # Download 2 per worker at a time
    RESCAN_INTERVAL = 30  # Seconds to wait before rescanning for new files
    MAX_EMPTY_SCANS = 2  # Stop after N consecutive empty scans (0 = never stop)
    
    # Resource safety thresholds for bulk import
    BULK_MEMORY_PAUSE_THRESHOLD = MEMORY_HIGH_THRESHOLD  # Pause downloads at this RAM %
    BULK_DISK_MIN_FREE_GB = 5  # Minimum free disk space to continue downloading
    BULK_CPU_PAUSE_THRESHOLD = 95  # Pause downloads when CPU > 95%
    
    consecutive_empty_scans = 0
    total_processed_all_time = 0
    scan_count = 0
    
    try:
        # Get fresh token (auto-refresh)
        dropbox_token = get_valid_dropbox_token()
        
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
        
        # =============================================================================
        # CONTINUOUS LOOP - Keep running until manually stopped
        # =============================================================================
        while True:
            scan_count += 1
            
            # Check for stop request
            with bulk_import_lock:
                if bulk_import_state['stop_requested']:
                    bulk_import_state['current_status'] = 'stopped'
                    bulk_import_state['active'] = False
                    bulk_import_state['last_update'] = time.time()
                    print("⏹️ Bulk import stopped by user")
                    return
            
            # Refresh token before each scan cycle
            dropbox_token = get_valid_dropbox_token()
            headers['Authorization'] = f'Bearer {dropbox_token}'
        
            # PHASE 1: Scan for all files
            with bulk_import_lock:
                bulk_import_state['current_status'] = 'scanning'
                bulk_import_state['last_update'] = time.time()
            
            print(f"\n{'='*60}")
            print(f"🔍 SCAN #{scan_count} - Scanning '{folder_path or '(root)'}' recursively...")
            print(f"{'='*60}")
            
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
                
                try:
                    if cursor:
                        response = requests.post(
                            'https://api.dropboxapi.com/2/files/list_folder/continue',
                            headers=headers,
                            json={'cursor': cursor},
                            timeout=60
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
                            },
                            timeout=60
                        )
                except requests.exceptions.RequestException as e:
                    print(f"⚠️ Network error during scan: {e} - will retry in {RESCAN_INTERVAL}s")
                    time.sleep(RESCAN_INTERVAL)
                    # Refresh token and retry
                    dropbox_token = get_valid_dropbox_token()
                    headers['Authorization'] = f'Bearer {dropbox_token}'
                    cursor = None  # Reset cursor to restart scan from scratch
                    all_files = []  # Reset files list
                    continue  # Retry scan
                
                # Handle token expiration - refresh and retry
                if response.status_code == 401 or is_token_expired_error(response):
                    print("🔄 Token expired during scan, refreshing...")
                    dropbox_token = get_valid_dropbox_token()
                    headers['Authorization'] = f'Bearer {dropbox_token}'
                    continue  # Retry the request
                
                if response.status_code != 200:
                    try:
                        error_msg = response.json().get('error_summary', 'Unknown error') if response.text else 'Unknown error'
                    except (ValueError, KeyError):
                        error_msg = f'HTTP {response.status_code}: {response.text[:200] if response.text else "Unknown error"}'
                    print(f"⚠️ Scan error: {error_msg} - will retry in {RESCAN_INTERVAL}s")
                    time.sleep(RESCAN_INTERVAL)
                    continue  # Retry scan
                
                try:
                    result = response.json()
                except ValueError:
                    print(f"⚠️ Invalid JSON response from Dropbox - will retry in {RESCAN_INTERVAL}s")
                    time.sleep(RESCAN_INTERVAL)
                    continue  # Retry scan
                
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
                
                # Only update scanning_found during pagination (total_found stays stable)
                with bulk_import_lock:
                    bulk_import_state['total_scanned'] += len(result.get('entries', []))
                    bulk_import_state['scanning_found'] = len(all_files)
                    bulk_import_state['files_queue'] = all_files.copy()
                    bulk_import_state['last_update'] = time.time()
                
                has_more = result.get('has_more', False)
                cursor = result.get('cursor')
            
            # Scan complete: update total_found (this is the stable number shown in the UI)
            with bulk_import_lock:
                bulk_import_state['total_found'] = len(all_files)
                bulk_import_state['last_update'] = time.time()
            
            print(f"📦 Scan complete: {len(all_files)} audio files found")
            
            # If no files found, wait and rescan
            if len(all_files) == 0:
                consecutive_empty_scans += 1
                
                # Check if we should stop (MAX_EMPTY_SCANS = 0 means never stop)
                if MAX_EMPTY_SCANS > 0 and consecutive_empty_scans >= MAX_EMPTY_SCANS:
                    with bulk_import_lock:
                        bulk_import_state['current_status'] = 'complete'
                        bulk_import_state['active'] = False
                        bulk_import_state['last_update'] = time.time()
                    print(f"\n{'='*60}")
                    print(f"✅ BULK IMPORT COMPLETE - All files processed!")
                    print(f"   Processed: {bulk_import_state.get('processed', 0)}")
                    print(f"   Skipped: {bulk_import_state.get('skipped', 0)}")
                    print(f"   Failed: {bulk_import_state.get('failed', 0)}")
                    print(f"{'='*60}")
                    return
                
                with bulk_import_lock:
                    bulk_import_state['current_status'] = 'watching'
                    bulk_import_state['current_file'] = f'🔄 Verifying no remaining files... (check #{consecutive_empty_scans}/{MAX_EMPTY_SCANS})'
                    bulk_import_state['last_update'] = time.time()
                
                print(f"🔄 No files found (check {consecutive_empty_scans}/{MAX_EMPTY_SCANS}) - rechecking in {RESCAN_INTERVAL}s...")
                time.sleep(RESCAN_INTERVAL)
                continue  # Go back to start of while loop to rescan
            
            # Reset empty scan counter when files found
            consecutive_empty_scans = 0
            
            # =============================================================================
            # SMART PIPELINE: Download + Process with Buffer
            # =============================================================================
        
            # Use the GLOBAL session for bulk import (so it shows in All Tracks)
            bulk_session_id = 'global'

            # Shared state for pipeline
            dropbox_paths = {}  # safe_filename -> dropbox_path
            dropbox_paths_lock = Lock()
            completed_tracks = set()
            completed_lock = Lock()
            files_to_process = list(all_files)  # Queue of files still to download
            files_lock = Lock()
            download_index = [0]  # Track which file we're on

            from concurrent.futures import ThreadPoolExecutor, as_completed

            print(f"\n{'='*60}")
            print(f"🚀 SMART PIPELINE STARTED")
            print(f"   Total files: {len(all_files)}")
            print(f"   Buffer size: {BUFFER_SIZE} tracks")
            print(f"   Download batch: {DOWNLOAD_BATCH} at a time")
            print(f"   Workers: {config.NUM_WORKERS}")
            print(f"{'='*60}\n")

            def get_queue_size():
                """Get number of tracks waiting/processing in queue."""
                with queue_items_lock:
                    return sum(1 for info in queue_items.values() 
                              if info.get('status') in ('waiting', 'processing'))

            def download_single_file(file_info):
                """Download a single file from Dropbox."""
                file_name = file_info.get('name', 'unknown')
                current_index = 0
                try:
                    dropbox_path = file_info['path']

                    with files_lock:
                        current_index = download_index[0]
                        download_index[0] += 1

                    # Check for stop
                    with bulk_import_lock:
                        if bulk_import_state['stop_requested']:
                            return {'status': 'stopped', 'name': file_name}

                    # TITLE FILTERING - Skip tracks with banned keywords
                    title_result = process_track_title_for_import(file_name)

                    if title_result['skip']:
                        print(f"⏭️  [{current_index+1}/{len(all_files)}] SKIP: {file_name} ({title_result['skip_reason']})")

                        # Delete from Dropbox
                        delete_from_dropbox_if_skipped(dropbox_path, dropbox_token, dropbox_team_member_id, namespace_id)

                        with bulk_import_lock:
                            bulk_import_state['skipped'] += 1
                            bulk_import_state['skipped_files'].append({
                                'name': file_name,
                                'reason': title_result['skip_reason']
                            })
                            bulk_import_state['last_update'] = time.time()

                        return {'status': 'skipped', 'name': file_name}

                    # Prepare filename
                    cleaned_title = title_result['cleaned_title']
                    extension = os.path.splitext(file_name)[1]  # e.g. ".mp3"
                    # Remove extension from cleaned_title if already present (prevents .mp3.mp3)
                    cleaned_title_no_ext = os.path.splitext(cleaned_title)[0] if cleaned_title.lower().endswith(extension.lower()) else cleaned_title
                    safe_filename = re.sub(r'[^\w\s\-\.]', '', cleaned_title_no_ext).strip() or f'track_{current_index}'
                    safe_filename = safe_filename + extension
                    local_path = os.path.join(UPLOAD_FOLDER, safe_filename)

                    # Add to queue tracker immediately
                    with queue_items_lock:
                        queue_items[safe_filename] = {
                            'status': 'waiting',
                            'worker': None,
                            'progress': 0,
                            'session_id': bulk_session_id,
                            'step': '⬇️ Downloading...',
                            'added_at': time.time(),
                            'processing_started_at': None
                        }

                    # Download
                    download_headers = {
                        'Authorization': f'Bearer {dropbox_token}',
                        'Dropbox-API-Arg': json.dumps({'path': dropbox_path})
                    }
                    if dropbox_team_member_id:
                        download_headers['Dropbox-API-Select-User'] = dropbox_team_member_id
                    if namespace_id:
                        download_headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})

                    response = requests.post(
                        'https://content.dropboxapi.com/2/files/download',
                        headers=download_headers,
                        stream=True
                    )

                    if response.status_code != 200:
                        raise Exception(f'HTTP {response.status_code}')

                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    # Store for Dropbox deletion
                    with dropbox_paths_lock:
                        dropbox_paths[safe_filename] = dropbox_path

                    # Update state (both global and per-iteration counters)
                    with bulk_import_lock:
                        bulk_import_state['downloaded'] += 1
                        bulk_import_state['last_update'] = time.time()
                    
                    # Thread-safe increment of per-iteration counter
                    nonlocal iteration_downloaded
                    with files_lock:
                        iteration_downloaded += 1

                    # Queue for processing
                    with queue_items_lock:
                        if safe_filename in queue_items:
                            queue_items[safe_filename]['step'] = 'En attente...'

                    track_queue.put({
                        'filename': safe_filename,
                        'session_id': bulk_session_id,
                        'is_retry': False
                    })

                    print(f"✅ [{current_index+1}/{len(all_files)}] {safe_filename}")
                    return {'status': 'ok', 'name': file_name, 'safe_filename': safe_filename}

                except Exception as e:
                    print(f"❌ [{current_index+1}/{len(all_files)}] {file_name}: {str(e)[:100]}")
                    with bulk_import_lock:
                        bulk_import_state['failed'] += 1
                        bulk_import_state['failed_files'].append({'name': file_name, 'error': str(e)})
                        bulk_import_state['last_update'] = time.time()
                    return {'status': 'failed', 'name': file_name, 'error': str(e)}

            def move_to_done_in_dropbox(filename):
                """Move a file to /track done folder in Dropbox after successful processing."""
                with dropbox_paths_lock:
                    dropbox_path = dropbox_paths.get(filename)

                if not dropbox_path:
                    return

                try:
                    move_headers = {
                        'Authorization': f'Bearer {dropbox_token}',
                        'Content-Type': 'application/json'
                    }
                    if dropbox_team_member_id:
                        move_headers['Dropbox-API-Select-User'] = dropbox_team_member_id
                    if namespace_id:
                        move_headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': namespace_id})

                    # Move to /track done/ folder instead of deleting
                    dest_filename = os.path.basename(dropbox_path)
                    dest_path = f"/track done/{dest_filename}"

                    response = requests.post(
                        'https://api.dropboxapi.com/2/files/move_v2',
                        headers=move_headers,
                        json={
                            'from_path': dropbox_path,
                            'to_path': dest_path,
                            'autorename': True,
                            'allow_ownership_transfer': False
                        }
                    )
                    if response.status_code == 200:
                        print(f"📦  Moved to /track done/: {filename}")
                    else:
                        # If move fails (e.g. folder doesn't exist), try creating it first
                        error_data = response.json() if response.text else {}
                        error_summary = error_data.get('error_summary', '')
                        if 'not_found' in error_summary:
                            # Create the /track done/ folder
                            requests.post(
                                'https://api.dropboxapi.com/2/files/create_folder_v2',
                                headers=move_headers,
                                json={'path': '/track done', 'autorename': False}
                            )
                            # Retry the move
                            retry_response = requests.post(
                                'https://api.dropboxapi.com/2/files/move_v2',
                                headers=move_headers,
                                json={
                                    'from_path': dropbox_path,
                                    'to_path': dest_path,
                                    'autorename': True,
                                    'allow_ownership_transfer': False
                                }
                            )
                            if retry_response.status_code == 200:
                                print(f"📦  Moved to /track done/: {filename}")
                            else:
                                print(f"⚠️  Could not move to /track done/: {retry_response.text[:200]}")
                        else:
                            print(f"⚠️  Could not move to /track done/: {error_summary}")
                except Exception as e:
                    print(f"⚠️  Could not move to /track done/: {e}")

            def check_completed_tracks():
                """Check for completed tracks and move to /track done/ in Dropbox."""
                with dropbox_paths_lock:
                    filenames_to_check = list(dropbox_paths.keys())

                for filename in filenames_to_check:
                    with completed_lock:
                        if filename in completed_tracks:
                            continue

                    with queue_items_lock:
                        info = queue_items.get(filename, {})
                        status = info.get('status', '')

                        # Check if no longer in queue (processed and removed)
                        if filename not in queue_items:
                            with completed_lock:
                                if filename not in completed_tracks:
                                    completed_tracks.add(filename)
                                    with bulk_import_lock:
                                        bulk_import_state['processed'] += 1
                                        bulk_import_state['completed_files'].append(filename)
                                        bulk_import_state['last_update'] = time.time()
                                    move_to_done_in_dropbox(filename)

                        elif status == 'failed':
                            with completed_lock:
                                if filename not in completed_tracks:
                                    completed_tracks.add(filename)
                                    print(f"❌ Failed (kept in Dropbox): {filename}")

            # =============================================================================
            # MAIN PIPELINE LOOP - Download with buffer, process continuously
            # =============================================================================

            with bulk_import_lock:
                bulk_import_state['current_status'] = 'downloading'
                bulk_import_state['last_update'] = time.time()

            file_index = 0
            iteration_downloaded = 0  # Per-iteration download counter (NOT accumulated across rescans)
            download_threads = min(config.NUM_WORKERS, 10)  # Limit concurrent downloads

            print(f"🚀 Starting pipeline with {download_threads} download threads")

            try:
                while True:
                    # Check for stop
                    with bulk_import_lock:
                        if bulk_import_state['stop_requested']:
                            bulk_import_state['current_status'] = 'stopped'
                            bulk_import_state['active'] = False
                            print("⏹️ Bulk import stopped")
                            break

                    # Get current queue size (waiting + processing)
                    current_queue_size = get_queue_size()

                    # Check completed tracks and move to /track done/ in Dropbox
                    check_completed_tracks()

                    # Update status display
                    with bulk_import_lock:
                        downloaded = bulk_import_state['downloaded']
                        processed = bulk_import_state['processed']
                        skipped = bulk_import_state['skipped']
                        failed = bulk_import_state['failed']
                        bulk_import_state['current_file'] = f'⬇️ {downloaded} | ⏳ {current_queue_size} queue | ✅ {processed} done'
                        bulk_import_state['last_update'] = time.time()

                    # Check if current batch is all done
                    # Use per-iteration counter (NOT global downloaded which accumulates across rescans)
                    with completed_lock:
                        total_complete = len(completed_tracks)

                    if file_index >= len(all_files) and total_complete >= iteration_downloaded:
                        total_processed_all_time += bulk_import_state['processed']
                        print(f"\n✅ Batch complete! Total processed this session: {total_processed_all_time}")
                        print(f"🔄 Rescanning folder to check for remaining files...")

                        # Reset batch counters but keep running
                        with bulk_import_lock:
                            bulk_import_state['current_status'] = 'watching'
                            bulk_import_state['current_file'] = f'🔄 Batch done ({total_processed_all_time} processed) - rescanning folder...'
                            bulk_import_state['last_update'] = time.time()

                        time.sleep(RESCAN_INTERVAL)
                        break  # Break inner loop to rescan

                    # ===== RESOURCE SAFETY CHECKS before downloading =====
                    resource_ok = True
                    
                    # Check RAM - pause downloads if memory is high
                    mem_percent = get_memory_percent()
                    if mem_percent >= BULK_MEMORY_PAUSE_THRESHOLD:
                        resource_ok = False
                        if mem_percent >= MEMORY_CRITICAL_THRESHOLD:
                            print(f"🔴 BULK IMPORT: CRITICAL RAM {mem_percent:.1f}% - pausing downloads, forcing GC")
                            force_garbage_collect("Bulk import critical RAM")
                        else:
                            print(f"⚠️ BULK IMPORT: RAM {mem_percent:.1f}% >= {BULK_MEMORY_PAUSE_THRESHOLD}% - pausing downloads")
                        with bulk_import_lock:
                            bulk_import_state['current_file'] = f'⏸️ RAM high ({mem_percent:.0f}%) - waiting for workers to free memory...'
                            bulk_import_state['last_update'] = time.time()
                    
                    # Check disk space - pause if running low
                    if resource_ok:
                        try:
                            disk_free_gb = psutil.disk_usage('/').free / (1024**3)
                            if disk_free_gb < BULK_DISK_MIN_FREE_GB:
                                resource_ok = False
                                print(f"⚠️ BULK IMPORT: Disk space low ({disk_free_gb:.1f}GB free) - pausing downloads")
                                with bulk_import_lock:
                                    bulk_import_state['current_file'] = f'⏸️ Disk space low ({disk_free_gb:.1f}GB free) - waiting...'
                                    bulk_import_state['last_update'] = time.time()
                        except Exception:
                            pass  # Skip disk check on error
                    
                    # Check CPU - pause if system is overloaded
                    if resource_ok:
                        try:
                            cpu_percent = psutil.cpu_percent(interval=0)
                            if cpu_percent >= BULK_CPU_PAUSE_THRESHOLD:
                                resource_ok = False
                                print(f"⚠️ BULK IMPORT: CPU {cpu_percent:.0f}% >= {BULK_CPU_PAUSE_THRESHOLD}% - pausing downloads")
                                with bulk_import_lock:
                                    bulk_import_state['current_file'] = f'⏸️ CPU high ({cpu_percent:.0f}%) - waiting for processing to finish...'
                                    bulk_import_state['last_update'] = time.time()
                        except Exception:
                            pass  # Skip CPU check on error

                    # DOWNLOAD LOGIC: Only download if buffer has room AND resources are OK
                    if resource_ok and current_queue_size < BUFFER_SIZE and file_index < len(all_files):
                        # Calculate how many we can download
                        room_in_buffer = BUFFER_SIZE - current_queue_size
                        files_remaining = len(all_files) - file_index
                        batch_size = min(DOWNLOAD_BATCH, room_in_buffer, files_remaining)

                        if batch_size > 0:
                            batch_files = all_files[file_index:file_index + batch_size]
                            file_index += batch_size

                            print(f"\n📥 Downloading {len(batch_files)} files (buffer: {current_queue_size}/{BUFFER_SIZE}, remaining: {files_remaining - batch_size}) [RAM: {get_memory_percent():.0f}%]")

                            with bulk_import_lock:
                                bulk_import_state['current_status'] = 'downloading'

                            # Download this batch in parallel (but limited)
                            with ThreadPoolExecutor(max_workers=download_threads) as batch_executor:
                                futures = [batch_executor.submit(download_single_file, f) for f in batch_files]
                                for future in as_completed(futures):
                                    try:
                                        result = future.result()
                                        if result.get('status') == 'stopped':
                                            break
                                    except Exception as future_err:
                                        print(f"⚠️ Download future error: {future_err}")
                                        # Continue with other downloads - don't crash the pipeline

                            with bulk_import_lock:
                                bulk_import_state['current_status'] = 'processing'

                            # After download, check queue again immediately
                            continue

                    # If buffer is full or no more files, just wait and monitor
                    if current_queue_size >= BUFFER_SIZE:
                        print(f"⏸️  Buffer full ({current_queue_size}/{BUFFER_SIZE}), waiting for workers...")

                    time.sleep(3)  # Wait before checking again

            except Exception as e:
                print(f"❌ Pipeline error: {e}")
                import traceback
                traceback.print_exc()
                # Don't stop on error - wait and retry
                print(f"🔄 Will retry in {RESCAN_INTERVAL}s...")
                time.sleep(RESCAN_INTERVAL)

                # Continue to next scan iteration (outer while loop)

    except Exception as e:
        import traceback
        print(f"❌ Bulk import fatal error: {str(e)}")
        print(traceback.format_exc())
        with bulk_import_lock:
            bulk_import_state['error'] = str(e)
            bulk_import_state['current_status'] = 'error'
            bulk_import_state['active'] = False
            bulk_import_state['last_update'] = time.time()
    
    # Final summary (only shown when manually stopped)
    print(f"\n{'='*60}")
    print(f"⏹️ BULK IMPORT STOPPED")
    print(f"   Total scans: {scan_count}")
    print(f"   Total processed: {total_processed_all_time}")
    print(f"   Skipped: {bulk_import_state.get('skipped', 0)}")
    print(f"   Failed: {bulk_import_state.get('failed', 0)}")
    print(f"{'='*60}")


@dropbox_bp.route('/dropbox/import', methods=['POST'])
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
    
    # Get valid token (auto-refreshes if expired)
    dropbox_token = get_valid_dropbox_token()
    dropbox_team_member_id = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '') or DROPBOX_TEAM_MEMBER_ID
    
    if not dropbox_token:
        return jsonify({'error': 'Dropbox not configured. Set DROPBOX_REFRESH_TOKEN in .env'}), 400
    
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
    Uses SMART PIPELINE: downloads only what workers can handle, then more as they finish.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    print(f"📦 Smart pipeline started with namespace: {root_namespace_id}")
    print(f"   Total files: {len(files_to_import)}")
    print(f"   Workers: {config.NUM_WORKERS}")
    
    # Create session-specific upload folder
    session_upload_folder = os.path.join(UPLOAD_FOLDER, session_id)
    os.makedirs(session_upload_folder, exist_ok=True)
    
    # Pipeline settings - download what workers can handle + small buffer
    BUFFER_SIZE = config.NUM_WORKERS * 2  # Keep 2x workers worth of tracks ready
    DOWNLOAD_BATCH = config.NUM_WORKERS   # Download in batches of config.NUM_WORKERS
    
    # Track state
    file_index = 0
    downloaded_count = 0
    queued_count = 0
    failed_count = 0
    
    def get_current_queue_size():
        """Get number of tracks waiting/processing for this session."""
        with queue_items_lock:
            return sum(1 for info in queue_items.values() 
                      if info.get('session_id') == session_id and 
                         info.get('status') in ('waiting', 'processing'))
    
    def download_and_queue_single(file_info):
        """Download one file and immediately queue it."""
        nonlocal downloaded_count, queued_count, failed_count
        
        file_path = file_info['path']
        file_name = file_info['name']
        
        try:
            # Update status to downloading
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
            safe_filename = safe_filename.strip() or f'track_{downloaded_count}.mp3'
            local_path = os.path.join(session_upload_folder, safe_filename)
            
            with open(local_path, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            downloaded_count += 1
            
            # Update downloaded status
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['downloaded'] += 1
                    dropbox_imports[import_id]['files'][file_name]['status'] = 'downloaded'
                    dropbox_imports[import_id]['files'][file_name]['local_path'] = local_path
            
            print(f"📥 [{downloaded_count}/{len(files_to_import)}] Downloaded: {file_name}")
            
            # Check if already processed
            is_processed, _ = is_track_already_processed(safe_filename)
            if is_processed:
                print(f"⏭️ Already processed: {safe_filename}")
                with dropbox_imports_lock:
                    if import_id in dropbox_imports:
                        dropbox_imports[import_id]['files'][file_name]['status'] = 'skipped'
                return {'status': 'skipped', 'name': file_name}
            
            # Add to queue tracker for UI display
            add_to_queue_tracker(safe_filename, session_id)
            
            # Queue for processing
            track_queue.put({
                'filepath': local_path,
                'filename': safe_filename,
                'session_id': session_id,
                'priority': 0
            })
            
            queued_count += 1
            
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['queued'] += 1
                    dropbox_imports[import_id]['files'][file_name]['status'] = 'queued'
            
            print(f"📋 [{queued_count}/{len(files_to_import)}] Queued: {safe_filename}")
            return {'status': 'ok', 'name': file_name}
            
        except Exception as e:
            failed_count += 1
            print(f"❌ Failed: {file_name}: {str(e)[:50]}")
            with dropbox_imports_lock:
                if import_id in dropbox_imports:
                    dropbox_imports[import_id]['failed'] += 1
                    dropbox_imports[import_id]['files'][file_name]['status'] = 'failed'
                    dropbox_imports[import_id]['files'][file_name]['error'] = str(e)
                    dropbox_imports[import_id]['errors'].append({
                        'file': file_name,
                        'error': str(e)
                    })
            return {'status': 'failed', 'name': file_name, 'error': str(e)}
    
    # =============================================================================
    # SMART PIPELINE LOOP
    # =============================================================================
    
    try:
        while file_index < len(files_to_import):
            # Get current queue size
            current_queue_size = get_current_queue_size()
            
            # Only download more if buffer has room
            if current_queue_size < BUFFER_SIZE:
                # Calculate how many we can download
                room_in_buffer = BUFFER_SIZE - current_queue_size
                files_remaining = len(files_to_import) - file_index
                batch_size = min(DOWNLOAD_BATCH, room_in_buffer, files_remaining)
                
                if batch_size > 0:
                    batch_files = files_to_import[file_index:file_index + batch_size]
                    file_index += batch_size
                    
                    print(f"\n📥 Downloading batch of {len(batch_files)} files (buffer: {current_queue_size}/{BUFFER_SIZE})")
                    
                    # Update status
                    with dropbox_imports_lock:
                        if import_id in dropbox_imports:
                            dropbox_imports[import_id]['status'] = 'downloading'
                    
                    # Download batch in parallel (limited threads)
                    download_threads = min(config.NUM_WORKERS, 8)
                    with ThreadPoolExecutor(max_workers=download_threads) as executor:
                        futures = [executor.submit(download_and_queue_single, f) for f in batch_files]
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                print(f"⚠️ Thread error: {e}")
                    
                    # Update status to processing
                    with dropbox_imports_lock:
                        if import_id in dropbox_imports:
                            dropbox_imports[import_id]['status'] = 'processing'
                    
                    # Continue to check if we can download more
                    continue
            
            # Buffer is full, wait for workers to process some
            print(f"⏸️ Buffer full ({current_queue_size}/{BUFFER_SIZE}), waiting for workers...")
            time.sleep(2)
        
        # All files downloaded, mark as complete
        with dropbox_imports_lock:
            if import_id in dropbox_imports:
                dropbox_imports[import_id]['status'] = 'processing'
                dropbox_imports[import_id]['completed_at'] = time.time()
        
        print(f"\n✅ Dropbox import {import_id} complete!")
        print(f"   Downloaded: {downloaded_count}")
        print(f"   Queued: {queued_count}")
        print(f"   Failed: {failed_count}")
        
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        with dropbox_imports_lock:
            if import_id in dropbox_imports:
                dropbox_imports[import_id]['status'] = 'error'
                dropbox_imports[import_id]['errors'].append({
                    'file': 'pipeline',
                    'error': str(e)
                })


@dropbox_bp.route('/dropbox/status/<import_id>')
def dropbox_import_status(import_id):
    """Get status of a Dropbox import operation."""
    with dropbox_imports_lock:
        if import_id not in dropbox_imports:
            return jsonify({'error': 'Import not found'}), 404
        
        status = dropbox_imports[import_id].copy()
    
    return jsonify(status)


@dropbox_bp.route('/dropbox/status')
def dropbox_all_imports_status():
    """Get status of all Dropbox imports for current session."""
    session_id = get_session_id()
    
    with dropbox_imports_lock:
        session_imports = {
            k: v.copy() for k, v in dropbox_imports.items()
            if v.get('session_id') == session_id
        }
    
    # Check if token is available
    dropbox_token = get_valid_dropbox_token()
    
    return jsonify({
        'imports': session_imports,
        'dropbox_configured': bool(dropbox_token)
    })


@dropbox_bp.route('/dropbox/configured')
def dropbox_configured():
    """Check if Dropbox is configured."""
    # Reload .env in case token was added after startup
    load_dotenv(override=True)
    
    # Get valid token (auto-refreshes if expired)
    dropbox_token = get_valid_dropbox_token()
    
    return jsonify({
        'configured': bool(dropbox_token),
        'message': 'Dropbox is configured' if dropbox_token else 'Set DROPBOX_REFRESH_TOKEN, DROPBOX_APP_KEY, DROPBOX_APP_SECRET in .env'
    })


@dropbox_bp.route('/dropbox/namespaces')
def dropbox_get_namespaces():
    """
    Get available namespaces for Dropbox team accounts.
    This helps find team folders that require a specific namespace_id.
    """
    load_dotenv(override=True)
    
    # Get valid token (auto-refreshes if expired)
    dropbox_token = get_valid_dropbox_token()
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
