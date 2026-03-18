"""
Track repair routes – S3 homonym migration for a single track.

Allows the main app to run the same logic as fix_s3_homonyms.py on one track by ID.
"""
from flask import Blueprint, jsonify

repair_bp = Blueprint('repair', __name__)


@repair_bp.route('/repair/track/<track_id>', methods=['GET', 'POST'])
def repair_track(track_id):
    """
    Repair one track: migrate old flat S3 paths to ISRC-prefixed paths,
    or null DB fields when the flat file belongs to another artist (ISRC mismatch).

    Uses the same logic as fix_s3_homonyms.py (Dropbox → S3 fallback + ISRC check).
    """
    try:
        from fix_s3_homonyms import repair_track_by_id
    except ImportError:
        return jsonify({'ok': False, 'error': 'Repair module not available'}), 500

    dry_run = False  # Apply fixes
    result = repair_track_by_id(track_id, dry_run=dry_run)

    if not result.get('ok'):
        return jsonify(result), 400
    return jsonify(result), 200


@repair_bp.route('/repair/track/<track_id>/dry-run', methods=['GET', 'POST'])
def repair_track_dry_run(track_id):
    """Same as POST /repair/track/<id> but dry-run only (no DB/S3 changes)."""
    try:
        from fix_s3_homonyms import repair_track_by_id
    except ImportError:
        return jsonify({'ok': False, 'error': 'Repair module not available'}), 500

    result = repair_track_by_id(track_id, dry_run=True)
    if not result.get('ok') and 'error' in result:
        return jsonify(result), 400
    return jsonify(result), 200
