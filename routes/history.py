"""
History routes blueprint for IDByRivoli.

Handles upload history viewing, CSV download, and clearing.
"""
import io
import csv
from datetime import datetime

from flask import Blueprint, jsonify, Response

from services.queue_service import log_message
from utils.history import get_upload_history_list, clear_upload_history

history_bp = Blueprint('history', __name__)


@history_bp.route('/history')
def get_history():
    """Get the upload history as JSON."""
    history = get_upload_history_list()
    
    # Calculate stats
    total = len(history)
    completed = sum(1 for h in history if h['status'] == 'completed')
    failed = sum(1 for h in history if h['status'] == 'failed')
    pending = sum(1 for h in history if h['status'] in ['uploaded', 'processing'])
    
    return jsonify({
        'history': history,
        'stats': {
            'total': total,
            'completed': completed,
            'failed': failed,
            'pending': pending
        }
    })


@history_bp.route('/history/csv')
def download_history_csv():
    """Download the upload history as a CSV file."""
    history = get_upload_history_list()
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['filename', 'status', 'date', 'type', 'session_id', 'error'])
    writer.writeheader()
    
    for entry in history:
        writer.writerow(entry)
    
    # Create response
    output.seek(0)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=upload_history_{timestamp}.csv'}
    )


@history_bp.route('/history/clear', methods=['POST'])
def clear_history():
    """Clear the upload history."""
    clear_upload_history()
    log_message("🗑️ Upload history cleared")
    return jsonify({'message': 'History cleared'})
