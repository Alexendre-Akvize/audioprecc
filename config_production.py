"""
ID By Rivoli - Production Configuration
═══════════════════════════════════════════════════════════════════
OPTIMIZED FOR: H100 80GB VRAM | 20 vCPU | 240GB RAM | 5TB Scratch
═══════════════════════════════════════════════════════════════════

This configuration maximizes throughput for the above hardware specs.
"""

import os
import multiprocessing
import psutil

# =============================================================================
# SERVER CONFIGURATION
# =============================================================================

# Flask/Gunicorn settings
PORT = int(os.environ.get('PORT', 8888))
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
SECRET_KEY = os.environ.get('SECRET_KEY', 'idbyrivoli-secret-key-2024')

# Gunicorn workers (for web requests, not track processing)
# Keep low since heavy work is done in background threads
GUNICORN_WORKERS = int(os.environ.get('GUNICORN_WORKERS', 4))
GUNICORN_TIMEOUT = int(os.environ.get('GUNICORN_TIMEOUT', 300))

# =============================================================================
# GPU OPTIMIZATION (H100 80GB)
# =============================================================================

def detect_gpu_config():
    """Detect GPU and return optimal configuration."""
    import psutil
    
    config = {
        'device': 'cpu',
        'gpu_name': None,
        'gpu_memory_gb': 0,
        'ram_gb': psutil.virtual_memory().total / (1024**3),
        'cpu_count': multiprocessing.cpu_count(),
        'track_workers': 8,       # Parallel track processing workers
        'demucs_jobs': 4,         # Jobs per Demucs process (-j flag)
        'demucs_segment': 7.8,    # Segment size (max for htdemucs)
        'demucs_overlap': 0.1,    # Overlap between segments
        'edit_workers': 4,        # Parallel edit generation workers
    }
    
    try:
        import torch
        if torch.cuda.is_available():
            config['device'] = 'cuda'
            config['gpu_name'] = torch.cuda.get_device_name(0)
            config['gpu_memory_gb'] = torch.cuda.get_device_properties(0).total_memory / (1024**3)
            
            # H100 80GB + 240GB RAM - MAXIMUM THROUGHPUT
            if config['gpu_memory_gb'] >= 70 and config['ram_gb'] >= 200:
                config['track_workers'] = 16     # Process 16 tracks concurrently!
                config['demucs_jobs'] = 20       # Maximum parallelism within Demucs
                config['edit_workers'] = 16      # Max out edit generation
                
            # H100 80GB (lower RAM)
            elif config['gpu_memory_gb'] >= 70:
                config['track_workers'] = 12
                config['demucs_jobs'] = 16
                config['edit_workers'] = 8
                
            # A100 40-80GB
            elif config['gpu_memory_gb'] >= 40:
                config['track_workers'] = 8
                config['demucs_jobs'] = 12
                config['edit_workers'] = 6
                
            # RTX 3090/4090 (24GB)
            elif config['gpu_memory_gb'] >= 20:
                config['track_workers'] = 4
                config['demucs_jobs'] = 8
                config['edit_workers'] = 4
                
            # Lower-end GPUs
            else:
                config['track_workers'] = 2
                config['demucs_jobs'] = 4
                config['edit_workers'] = 2
    except ImportError:
        pass
    
    return config

GPU_CONFIG = detect_gpu_config()

# =============================================================================
# PROCESSING SETTINGS
# =============================================================================

# Number of parallel track processing workers
# Each worker handles one track through the full pipeline
TRACK_WORKERS = int(os.environ.get('TRACK_WORKERS', GPU_CONFIG['track_workers']))

# Demucs configuration
DEMUCS_MODEL = os.environ.get('DEMUCS_MODEL', 'htdemucs')
DEMUCS_DEVICE = GPU_CONFIG['device']
DEMUCS_JOBS = int(os.environ.get('DEMUCS_JOBS', GPU_CONFIG['demucs_jobs']))
DEMUCS_SEGMENT = float(os.environ.get('DEMUCS_SEGMENT', GPU_CONFIG['demucs_segment']))
DEMUCS_OVERLAP = float(os.environ.get('DEMUCS_OVERLAP', GPU_CONFIG['demucs_overlap']))

# Edit generation workers
EDIT_WORKERS = int(os.environ.get('EDIT_WORKERS', GPU_CONFIG['edit_workers']))

# MP3 export settings
MP3_BITRATE = os.environ.get('MP3_BITRATE', '320')

# =============================================================================
# MEMORY MANAGEMENT
# =============================================================================

# PyTorch memory settings (H100 specific)
PYTORCH_CUDA_ALLOC_CONF = "max_split_size_mb:512"

# Process limits - optimized for 1000+ track batch uploads
MAX_CONCURRENT_UPLOADS = int(os.environ.get('MAX_CONCURRENT_UPLOADS', 50))  # Concurrent upload connections
MAX_QUEUE_SIZE = int(os.environ.get('MAX_QUEUE_SIZE', 2000))  # Support 1000+ track batches

# Cleanup settings
AUTO_CLEANUP_AFTER_DOWNLOAD = os.environ.get('AUTO_CLEANUP', 'true').lower() == 'true'
CLEANUP_DELAY_SECONDS = int(os.environ.get('CLEANUP_DELAY', 0))

# =============================================================================
# API CONFIGURATION
# =============================================================================

API_ENDPOINT = os.environ.get('API_ENDPOINT', 'https://track.idbyrivoli.com/upload')
API_KEY = os.environ.get('API_KEY', '5X#JP5ifkSm?oE6@haMriYG$j!87BEfX@zg3CxcE')
API_TIMEOUT = int(os.environ.get('API_TIMEOUT', 30))

# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# =============================================================================
# PRINT CONFIGURATION
# =============================================================================

def print_config():
    """Print current configuration for debugging."""
    print("\n" + "="*60)
    print("ID BY RIVOLI - PRODUCTION CONFIGURATION")
    print("="*60)
    print(f"\n🖥️  SERVER:")
    print(f"   Port: {PORT}")
    print(f"   Gunicorn Workers: {GUNICORN_WORKERS}")
    print(f"   Timeout: {GUNICORN_TIMEOUT}s")
    
    print(f"\n🎮 GPU:")
    print(f"   Device: {GPU_CONFIG['device']}")
    print(f"   GPU Name: {GPU_CONFIG['gpu_name'] or 'N/A'}")
    print(f"   GPU Memory: {GPU_CONFIG['gpu_memory_gb']:.1f}GB")
    
    print(f"\n⚡ PROCESSING:")
    print(f"   Track Workers: {TRACK_WORKERS}")
    print(f"   Demucs Jobs: {DEMUCS_JOBS}")
    print(f"   Demucs Segment: {DEMUCS_SEGMENT}")
    print(f"   Edit Workers: {EDIT_WORKERS}")
    
    print(f"\n📦 THROUGHPUT ESTIMATE:")
    # H100 processes a 3-4 minute track in ~20-30 seconds
    if GPU_CONFIG['gpu_memory_gb'] >= 70 and GPU_CONFIG.get('ram_gb', 0) >= 200:
        tracks_per_min = TRACK_WORKERS * 2.5
        print(f"   🔥 ~{tracks_per_min:.0f} tracks/minute (H100 + 240GB RAM MAXIMUM)")
        print(f"   🔥 ~{tracks_per_min * 60:.0f} tracks/hour")
    elif GPU_CONFIG['gpu_memory_gb'] >= 70:
        print(f"   ~{TRACK_WORKERS * 2} tracks/minute (H100)")
    elif GPU_CONFIG['gpu_memory_gb'] >= 40:
        print(f"   ~{TRACK_WORKERS * 1.5:.0f} tracks/minute")
    else:
        print(f"   ~{TRACK_WORKERS} tracks/minute")
    
    print("="*60 + "\n")

if __name__ == '__main__':
    print_config()
