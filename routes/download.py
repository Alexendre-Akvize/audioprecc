"""
Download routes blueprint for IDByRivoli.

Handles file downloads, ZIP downloads, download confirmation, and file listing.
"""
import os
import io
import time
import shutil
import zipfile
import urllib.parse
from io import BytesIO

from flask import Blueprint, request, jsonify, send_file, abort

import config
from config import (
    PROCESSED_FOLDER,
    OUTPUT_FOLDER,
    UPLOAD_FOLDER,
    API_KEY,
    DELETION_DELAY_MINUTES,
    MAX_PENDING_TRACKS,
    PENDING_WARNING_THRESHOLD,
    DELETE_AFTER_DOWNLOAD,
    DELAYED_DELETE_ENABLED,
    SEQUENTIAL_MODE,
    track_download_status,
    track_download_status_lock,
    pending_downloads,
    pending_downloads_lock,
)
from services.queue_service import (
    get_session_id,
    log_message,
    job_status,
    log_file_download,
)
from utils.tracking import (
    mark_file_downloaded,
    get_pending_downloads_for_track,
    cleanup_track_after_downloads,
    schedule_track_deletion,
    confirm_track_download,
    get_track_download_status,
    get_pending_tracks_count,
    get_pending_tracks_list,
    check_pending_tracks_warning,
)
from utils.file_utils import get_already_processed_tracks

download_bp = Blueprint('download', __name__)


@download_bp.route('/download_all_zip')
def download_all_zip():
    """
    Creates a ZIP file containing all processed tracks and sends it to the user.
    Can be called at any time to get currently finished tracks.
    """
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


@download_bp.route('/download_file')
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


@download_bp.route('/confirm_download', methods=['POST', 'GET'])
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
    import json as json_module
    
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
            data = json_module.loads(request.data.decode('utf-8'))
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


@download_bp.route('/track_download_status')
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


@download_bp.route('/already_processed')
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


@download_bp.route('/pending_downloads')
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
@download_bp.route('/processed/<path:filepath>')
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
@download_bp.route('/list_files')
def list_files():
    """Debug route to see what files are available."""
    result = {}
    for subdir in os.listdir(PROCESSED_FOLDER):
        subdir_path = os.path.join(PROCESSED_FOLDER, subdir)
        if os.path.isdir(subdir_path):
            result[subdir] = os.listdir(subdir_path)
    return jsonify(result)
