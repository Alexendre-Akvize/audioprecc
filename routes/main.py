"""
Main routes blueprint for IDByRivoli.

Handles the index page and public URL detection.
"""
from flask import Blueprint, render_template, request

import config

main_bp = Blueprint('main', __name__)


@main_bp.before_app_request
def set_public_url():
    """Captures the current public URL from the request headers to support dynamic Pod URLs (RunPod, etc.)."""
    # Always try to get the best URL from headers on each request
    # Priority: X-Forwarded-Host > Host header > existing value
    forwarded_host = request.headers.get('X-Forwarded-Host')
    original_host = request.headers.get('Host')
    # Default to http since this server typically runs without SSL
    scheme = request.headers.get('X-Forwarded-Proto', 'http')
    
    # Debug: log all relevant headers on first request
    if not config.CURRENT_HOST_URL or 'localhost' in config.CURRENT_HOST_URL:
        print(f"🔍 Headers debug:")
        print(f"   X-Forwarded-Host: {forwarded_host}")
        print(f"   X-Forwarded-Proto: {scheme}")
        print(f"   Host: {original_host}")
        print(f"   X-Real-IP: {request.headers.get('X-Real-IP')}")
        print(f"   Origin: {request.headers.get('Origin')}")
        print(f"   Referer: {request.headers.get('Referer')}")
    
    new_url = None
    
    # RunPod and similar platforms set X-Forwarded-Host to the public URL
    if forwarded_host and 'localhost' not in forwarded_host:
        new_url = f"{scheme}://{forwarded_host}"
    # Try Origin header (set by browser on CORS requests)
    elif request.headers.get('Origin') and 'localhost' not in request.headers.get('Origin', ''):
        new_url = request.headers.get('Origin')
    # Try Referer header
    elif request.headers.get('Referer') and 'localhost' not in request.headers.get('Referer', ''):
        # Extract base URL from referer
        referer = request.headers.get('Referer')
        from urllib.parse import urlparse
        parsed = urlparse(referer)
        if parsed.netloc and 'localhost' not in parsed.netloc:
            new_url = f"{parsed.scheme}://{parsed.netloc}"
    # Use Host header only if it's not a private IP or localhost
    elif original_host and not original_host.startswith(('10.', '172.', '192.168.', '100.', 'localhost', '127.')):
        new_url = f"{scheme}://{original_host}"
    
    # Update if we found a valid public URL (not localhost)
    if new_url and 'localhost' not in new_url and new_url != config.CURRENT_HOST_URL:
        config.CURRENT_HOST_URL = new_url
        print(f"📍 Public URL détectée: {config.CURRENT_HOST_URL}")


@main_bp.route('/')
def index():
    from services.track_service import get_git_info
    version_info = get_git_info()
    return render_template('index.html', version_info=version_info)
