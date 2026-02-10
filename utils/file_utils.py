"""
File utility functions for IDByRivoli.

Filename cleaning, artist formatting, label mappings,
and track-processed detection.
"""
import os
import re

from config import PROCESSED_FOLDER, pending_downloads, pending_downloads_lock


def clean_filename(filename):
    """
    Cleans filename: removes underscores, specific patterns, and unnecessary IDs.
    Example: DJ_Mustard_ft.Travis_Scott-Whole_Lotta_Lovin_Edits_and_Intro_Outros-Radio_Edit-77055446
    Result: DJ Mustard ft. Travis Scott - Whole Lotta Lovin Edits and Intro Outros
    """
    name, ext = os.path.splitext(filename)
    name = name.replace('_', ' ')
    name = re.sub(r'-\d+$', '', name)
    name = re.sub(r'\.(?=[A-Z])', '. ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name, ext


def format_artists(artist_string):
    """
    Formats multiple artists with proper separators.
    - 2 artists: "Artist A & Artist B"
    - 3+ artists: "Artist A, Artist B, Artist C & Artist D"
    
    Handles various input separators: /, ;, feat., ft., and, &
    Ensures proper ASCII output (no unicode escapes like \\u0026)
    """
    if not artist_string:
        return artist_string
    
    # Convert to string and decode any unicode escapes
    normalized = str(artist_string)
    
    # Decode unicode escapes if present (e.g., \u0026 -> &)
    try:
        if '\\u' in normalized:
            normalized = normalized.encode().decode('unicode_escape')
    except:
        pass
    
    # Replace null character \u0000 (common ID3 multi-value separator) with our separator
    normalized = normalized.replace('\x00', '|||')
    
    # Normalize separators - replace common separators with a standard one
    # Replace "feat.", "ft.", "Feat.", "Ft." with separator
    normalized = re.sub(r'\s*(?:feat\.?|ft\.?|Feat\.?|Ft\.?)\s*', '|||', normalized, flags=re.IGNORECASE)
    # Replace "|", " / ", "/", " & ", " and ", ";" with separator
    normalized = re.sub(r'\s*\|\s*', '|||', normalized)  # Pipe separator
    normalized = re.sub(r'\s*/\s*', '|||', normalized)
    normalized = re.sub(r'\s*;\s*', '|||', normalized)
    normalized = re.sub(r'\s+&\s+', '|||', normalized)
    normalized = re.sub(r'\s+and\s+', '|||', normalized, flags=re.IGNORECASE)
    
    # Split by our separator
    artists = [a.strip() for a in normalized.split('|||') if a.strip()]
    
    if len(artists) == 0:
        return artist_string
    elif len(artists) == 1:
        return artists[0]
    elif len(artists) == 2:
        return f"{artists[0]} & {artists[1]}"
    else:
        # 3 or more: "A, B, C & D"
        return ', '.join(artists[:-1]) + ' & ' + artists[-1]


# Label mapping: sub-labels to parent labels
LABEL_MAPPINGS = {
    'Universal Music Group': [
        'PolyGram Music Publishing',
        'Rondor Music',
        'Edition Ricordi',
        'Decca Publishing',
        'Universal Music Publishing Classical',
        'Universal Music Publishing Production Music',
        'Universal Music Publishing France',
        'Universal Music Publishing UK',
        'Universal Music Publishing US',
        'Universal Music Publishing Germany',
        'Universal Music Publishing Benelux',
        'Universal Music Publishing Scandinavia',
        'Universal Music Publishing Latin America',
        'Universal Music Publishing Asia',
        'Eagle-i Music',
        'Global Dog Publishing',
        'Casablanca Media Publishing',
        'Criterion Music Corp',
        'Beechwood Music Corp',
        'Universal Songs of PolyGram International',
        'Abbey Road Masters',
        'Island Music Publishing',
        'Motown Music Publishing',
        'Def Jam Music Publishing',
        'Capitol Music Publishing',
    ],
    'Sony Music Group': [
        'EMI Music Publishing',
        'ATV Music Publishing',
        'Famous Music',
        'Jobete Music',
        'Sony/ATV Latin',
        'Sony/ATV Europe',
        'Sony/ATV Scandinavia',
        'Sony/ATV France',
        'Sony/ATV Germany',
        'Sony/ATV UK',
        'Sony/ATV US',
        'Sony Music Publishing Production Music',
        'Extreme Music',
        'Tree Publishing',
        'Firstcom Music',
        'Filmtrax',
        'EMI Production Music',
        'Motown Catalog Publishing',
        'Chrysalis Songs',
    ],
    'Warner Music Group': [
        'Chappell & Co',
        'Warner Chappell Production Music',
        'Blue Mountain Music',
        'CPP',
        'Copyright Protection Provider',
        'Warner Chappell France',
        'Warner Chappell UK',
        'Warner Chappell US',
        'Warner Chappell Germany',
        'Warner Chappell Benelux',
        'Warner Chappell Scandinavia',
        'Warner Chappell Latin America',
        'Warner Chappell Asia',
        # Ex-Alfred Music Publishing (now part of Warner)
        'Alfred Music Publishing',
        'Faber Music',
        'Imagem Music Group',
        'Boosey & Hawkes',
        'Birch Tree Music',
        'Non-Stop Music',
        'Music Sales Group',
    ],
}


def get_parent_label(sub_label):
    """
    Maps a sub-label to its parent label.
    Returns the parent label if matched, otherwise returns the original sub-label.
    Case-insensitive matching with flexibility for partial matches.
    """
    if not sub_label:
        return ''
    
    sub_label_clean = sub_label.strip()
    sub_label_lower = sub_label_clean.lower()
    
    for parent_label, sub_labels in LABEL_MAPPINGS.items():
        for known_sub in sub_labels:
            known_sub_lower = known_sub.lower()
            # Exact match (case-insensitive)
            if sub_label_lower == known_sub_lower:
                return parent_label
            # Partial match: sub_label contains known_sub or vice versa
            # Be careful to avoid false positives - require substantial match
            if len(known_sub_lower) >= 5:  # Only for longer names to avoid false positives
                if known_sub_lower in sub_label_lower or sub_label_lower in known_sub_lower:
                    # Additional check: at least 70% of characters match
                    shorter = min(len(known_sub_lower), len(sub_label_lower))
                    if shorter >= 5:
                        return parent_label
    
    # No match found, return original
    return sub_label_clean


def is_track_already_processed(filename):
    """
    Check if a track has already been processed.
    Returns (is_processed: bool, processed_dir: str or None)
    
    Checks:
    1. If the track folder exists in PROCESSED_FOLDER
    2. If the track is in pending_downloads (processed, waiting for download)
    """
    clean_name, _ = clean_filename(filename)
    
    # Check if track folder exists in PROCESSED_FOLDER
    track_folder = os.path.join(PROCESSED_FOLDER, clean_name)
    if os.path.exists(track_folder) and os.path.isdir(track_folder):
        # Check if it has actual files inside (not empty folder)
        files = [f for f in os.listdir(track_folder) if f.endswith(('.mp3', '.wav'))]
        if files:
            return True, track_folder
    
    # Check if track is in pending_downloads
    with pending_downloads_lock:
        if clean_name in pending_downloads:
            return True, pending_downloads[clean_name].get('processed_dir', track_folder)
    
    return False, None


def get_already_processed_tracks():
    """Get list of all track names that have already been processed."""
    processed_tracks = set()
    
    # From PROCESSED_FOLDER
    if os.path.exists(PROCESSED_FOLDER):
        for item in os.listdir(PROCESSED_FOLDER):
            item_path = os.path.join(PROCESSED_FOLDER, item)
            if os.path.isdir(item_path):
                # Check if it has actual files
                files = [f for f in os.listdir(item_path) if f.endswith(('.mp3', '.wav'))]
                if files:
                    processed_tracks.add(item)
    
    # From pending_downloads
    with pending_downloads_lock:
        processed_tracks.update(pending_downloads.keys())
    
    return list(processed_tracks)
