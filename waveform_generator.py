"""
Waveform generator for audio files.
Generates peak data matching the JavaScript implementation for visualization.
"""

import numpy as np
from typing import Optional, Union
from pathlib import Path
import io


def generate_waveform(
    input_data: Union[str, bytes, Path],
    sample_size: int = 1024
) -> dict:
    """
    Generate waveform peak data from an audio file.
    
    Matches the JavaScript implementation:
    - Divides audio into segments of `sample_size` samples
    - For each segment, finds the peak (max absolute value)
    - Returns array of peaks (0.0 to 1.0) and duration in seconds
    
    Args:
        input_data: File path (str/Path) or audio bytes
        sample_size: Number of samples per segment (default: 1024)
    
    Returns:
        dict with 'waveform' (list of floats) and 'duration' (float in seconds)
    """
    from pydub import AudioSegment
    
    # Load audio
    if isinstance(input_data, (str, Path)):
        # Load from file path
        file_path = str(input_data)
        if file_path.lower().endswith('.mp3'):
            audio = AudioSegment.from_mp3(file_path)
        elif file_path.lower().endswith('.wav'):
            audio = AudioSegment.from_wav(file_path)
        elif file_path.lower().endswith('.flac'):
            audio = AudioSegment.from_file(file_path, format='flac')
        else:
            audio = AudioSegment.from_file(file_path)
    else:
        # Load from bytes
        audio = AudioSegment.from_file(io.BytesIO(input_data))
    
    # Convert to mono and get raw samples
    audio = audio.set_channels(1)
    
    # Get sample array (16-bit signed integers)
    samples = np.array(audio.get_array_of_samples(), dtype=np.float32)
    
    # Normalize to -1.0 to 1.0 range
    max_val = 32768.0  # 2^15 for 16-bit audio
    samples = samples / max_val
    
    # Calculate peaks for each segment
    peaks = []
    for i in range(0, len(samples), sample_size):
        segment = samples[i:i + sample_size]
        if len(segment) > 0:
            peak = float(np.max(np.abs(segment)))
            peaks.append(peak)
    
    # Get duration in seconds
    duration = len(audio) / 1000.0  # pydub duration is in milliseconds
    
    return {
        'waveform': peaks,
        'duration': duration
    }


def generate_waveform_from_url(
    url: str,
    sample_size: int = 1024,
    timeout: int = 300
) -> Optional[dict]:
    """
    Download audio from URL and generate waveform.
    
    Args:
        url: URL to download audio from
        sample_size: Number of samples per segment
        timeout: Download timeout in seconds
    
    Returns:
        dict with 'waveform' and 'duration', or None on error
    """
    import requests
    
    try:
        print(f"   📊 Downloading for waveform: {url[:80]}...")
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()
        
        audio_bytes = response.content
        print(f"   📊 Generating waveform from {len(audio_bytes) / 1024 / 1024:.2f} MB...")
        
        result = generate_waveform(audio_bytes, sample_size)
        print(f"   📊 Waveform generated: {len(result['waveform'])} peaks, {result['duration']:.2f}s duration")
        
        return result
        
    except Exception as e:
        print(f"   ⚠️ Waveform generation failed: {e}")
        return None


if __name__ == '__main__':
    # Test with a sample file
    import sys
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Generating waveform for: {file_path}")
        result = generate_waveform(file_path)
        print(f"Duration: {result['duration']:.2f}s")
        print(f"Peaks: {len(result['waveform'])} values")
        print(f"Sample peaks: {result['waveform'][:10]}")
    else:
        print("Usage: python waveform_generator.py <audio_file>")
