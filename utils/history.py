"""
Upload history tracking for IDByRivoli.

Manages a CSV-backed history of uploaded files with their statuses.
"""
import os
import csv
from datetime import datetime

from config import upload_history, upload_history_lock, HISTORY_FILE


def add_to_upload_history(filename, session_id, status='uploaded', track_type='Unknown', error=None):
    """Add or update a file in the upload history."""
    with upload_history_lock:
        upload_history[filename] = {
            'filename': filename,
            'status': status,
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'type': track_type,
            'session_id': session_id,
            'error': error
        }
        # Save to CSV file for persistence
        save_history_to_csv()


def update_upload_history_status(filename, status, track_type=None, error=None):
    """Update the status of a file in the upload history."""
    with upload_history_lock:
        if filename in upload_history:
            upload_history[filename]['status'] = status
            upload_history[filename]['date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if track_type:
                upload_history[filename]['type'] = track_type
            if error:
                upload_history[filename]['error'] = error
            save_history_to_csv()


def save_history_to_csv():
    """Save upload history to CSV file (called within lock)."""
    try:
        with open(HISTORY_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['filename', 'status', 'date', 'type', 'session_id', 'error'])
            writer.writeheader()
            for entry in upload_history.values():
                writer.writerow(entry)
    except Exception as e:
        print(f"⚠️ Could not save history to CSV: {e}")


def load_history_from_csv():
    """Load upload history from CSV file on startup."""
    global upload_history
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    upload_history[row['filename']] = row
            print(f"📋 Loaded {len(upload_history)} entries from upload history")
        except Exception as e:
            print(f"⚠️ Could not load history from CSV: {e}")


def get_upload_history_list():
    """Get the upload history as a sorted list (newest first)."""
    with upload_history_lock:
        entries = list(upload_history.values())
        # Sort by date descending
        entries.sort(key=lambda x: x['date'], reverse=True)
        return entries


def clear_upload_history():
    """Clear all upload history."""
    global upload_history
    with upload_history_lock:
        upload_history = {}
        if os.path.exists(HISTORY_FILE):
            os.remove(HISTORY_FILE)
