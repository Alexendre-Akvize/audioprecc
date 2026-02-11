"""
Dropbox service for IDByRivoli.

Token management, automatic refresh, and API request helper with retry.
"""
import os
import time
import requests

from config import (
    DROPBOX_ACCESS_TOKEN,
    DROPBOX_REFRESH_TOKEN,
    DROPBOX_APP_KEY,
    DROPBOX_APP_SECRET,
    dropbox_token_lock,
)
import config


def get_valid_dropbox_token():
    """
    Get a valid Dropbox access token, refreshing if necessary.
    Returns the current access token or refreshes it using the refresh token.
    """
    with dropbox_token_lock:
        current_time = time.time()
        
        # If token is still valid (with 5 min buffer), return it
        if config.dropbox_current_token and config.dropbox_token_expires_at > current_time + 300:
            return config.dropbox_current_token
        
        # Token expired or missing — attempt refresh
        if config.dropbox_current_token and config.dropbox_token_expires_at > 0:
            expired_ago = current_time - config.dropbox_token_expires_at
            print(f"⚠️ Dropbox token expired {expired_ago:.0f}s ago — refreshing...")
        
        # Try to refresh the token
        refresh_token = os.environ.get('DROPBOX_REFRESH_TOKEN', '') or DROPBOX_REFRESH_TOKEN
        app_key = os.environ.get('DROPBOX_APP_KEY', '') or DROPBOX_APP_KEY
        app_secret = os.environ.get('DROPBOX_APP_SECRET', '') or DROPBOX_APP_SECRET
        
        if not refresh_token:
            print("❌ DROPBOX_REFRESH_TOKEN not set — cannot refresh token!")
        if not app_key:
            print("❌ DROPBOX_APP_KEY not set — cannot refresh token!")
        if not app_secret:
            print("❌ DROPBOX_APP_SECRET not set — cannot refresh token!")
        
        if refresh_token and app_key and app_secret:
            try:
                print("🔄 Refreshing Dropbox access token...")
                response = requests.post(
                    'https://api.dropbox.com/oauth2/token',
                    data={
                        'grant_type': 'refresh_token',
                        'refresh_token': refresh_token,
                    },
                    auth=(app_key, app_secret),
                    timeout=30,
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    new_token = token_data.get('access_token', '')
                    if not new_token:
                        print("❌ Token refresh returned empty access_token!")
                    else:
                        config.dropbox_current_token = new_token
                        expires_in = token_data.get('expires_in', 14400)  # Default 4 hours
                        config.dropbox_token_expires_at = current_time + expires_in
                        
                        # Update environment variable for this session
                        os.environ['DROPBOX_ACCESS_TOKEN'] = config.dropbox_current_token
                        
                        print(f"✅ Dropbox token refreshed! Expires in {expires_in // 3600}h {(expires_in % 3600) // 60}m")
                        return config.dropbox_current_token
                else:
                    print(f"❌ Token refresh failed: HTTP {response.status_code} - {response.text[:300]}")
            except requests.exceptions.Timeout:
                print("❌ Token refresh timed out (30s) — Dropbox API may be down")
            except requests.exceptions.ConnectionError as e:
                print(f"❌ Token refresh connection error — no network? {e}")
            except Exception as e:
                print(f"❌ Token refresh error: {e}")
        
        # Fallback to current token (might be expired — caller should handle 401)
        current_token = os.environ.get('DROPBOX_ACCESS_TOKEN', '') or DROPBOX_ACCESS_TOKEN
        if current_token:
            print(f"⚠️ Using fallback token (may be expired) — length={len(current_token)}")
            config.dropbox_current_token = current_token
            return config.dropbox_current_token
        
        print("❌ No Dropbox token available at all!")
        return ''


def is_token_expired_error(response):
    """Check if a Dropbox API response indicates an expired token."""
    if response.status_code == 401:
        return True
    if response.status_code == 400:
        try:
            error_data = response.json()
            error_summary = error_data.get('error_summary', '').lower()
            if 'expired' in error_summary or 'invalid_access_token' in error_summary:
                return True
        except:
            pass
    return False


def dropbox_api_request(method, url, **kwargs):
    """
    Make a Dropbox API request with automatic token refresh on expiration.
    """
    # Get current valid token
    token = get_valid_dropbox_token()
    if not token:
        raise Exception("No Dropbox token available")
    
    # Update Authorization header
    headers = kwargs.get('headers', {})
    headers['Authorization'] = f'Bearer {token}'
    kwargs['headers'] = headers
    
    # Make the request
    response = requests.request(method, url, **kwargs)
    
    # If token expired, refresh and retry once
    if is_token_expired_error(response):
        print("⚠️ Token expired, refreshing...")
        config.dropbox_token_expires_at = 0  # Force refresh
        
        token = get_valid_dropbox_token()
        if token:
            headers['Authorization'] = f'Bearer {token}'
            response = requests.request(method, url, **kwargs)
    
    return response
