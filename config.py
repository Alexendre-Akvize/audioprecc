"""
Centralized configuration for IDByRivoli.

All constants, environment variables, Flask app initialization,
and shared mutable state (locks, queues, dictionaries) live here.
"""
import os
import threading
import queue
import multiprocessing
from threading import Lock

from dotenv import load_dotenv
load_dotenv()

# =============================================================================
# FIX TORCHAUDIO: Patch torchcodec ImportError at startup
# =============================================================================
def _patch_torchaudio_on_disk():
    """Patch torchaudio's _torchcodec.py to fall back to soundfile."""
    try:
        import torchaudio
        tc_path = os.path.join(os.path.dirname(torchaudio.__file__), '_torchcodec.py')
        if not os.path.exists(tc_path):
            return
        with open(tc_path, 'r') as f:
            content = f.read()
        if 'PATCHED_BY_IDBYRIVOLI' in content:
            return
        if 'raise ImportError' not in content:
            return
        lines = content.split('\n')
        new_lines = []
        i = 0
        patched = False
        while i < len(lines):
            line = lines[i]
            if 'raise ImportError' in line and 'torchcodec' in content.lower() and not patched:
                indent = len(line) - len(line.lstrip())
                spaces = ' ' * indent
                paren_depth = line.count('(') - line.count(')')
                i += 1
                while i < len(lines) and paren_depth > 0:
                    paren_depth += lines[i].count('(') - lines[i].count(')')
                    i += 1
                new_lines.append(f'{spaces}# PATCHED_BY_IDBYRIVOLI: Fall back to soundfile instead of crashing')
                new_lines.append(f'{spaces}import soundfile as _sf')
                new_lines.append(f'{spaces}import torch as _torch')
                new_lines.append(f'{spaces}_data, _sr = _sf.read(str(uri), dtype="float32")')
                new_lines.append(f'{spaces}if _data.ndim == 1:')
                new_lines.append(f'{spaces}    return _torch.from_numpy(_data).unsqueeze(0), _sr')
                new_lines.append(f'{spaces}else:')
                new_lines.append(f'{spaces}    return _torch.from_numpy(_data.T), _sr')
                patched = True
            else:
                new_lines.append(line)
                i += 1
        if patched:
            with open(tc_path, 'w') as f:
                f.write('\n'.join(new_lines))
            print(f"✅ Auto-patched torchaudio: {tc_path}")
        else:
            print(f"⚠️ Could not auto-patch torchaudio. Run: pip install torchcodec")
    except ImportError:
        pass
    except PermissionError:
        print(f"⚠️ Cannot patch torchaudio (permission denied). Run: pip install torchcodec")
    except Exception as e:
        print(f"⚠️ torchaudio patch failed: {e}")

_patch_torchaudio_on_disk()

# =============================================================================
# FLASK APP
# =============================================================================
from flask import Flask

app = Flask(__name__, template_folder='templates', static_folder='static')
app.secret_key = os.environ.get('SECRET_KEY', 'idbyrivoli-secret-key-2024')
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024
app.config['MAX_FORM_MEMORY_SIZE'] = 100 * 1024 * 1024

# =============================================================================
# PATHS
# =============================================================================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'output')
PROCESSED_FOLDER = os.path.join(BASE_DIR, 'processed')
DROPBOX_FOLDER = os.path.join(BASE_DIR, 'dropbox_downloads')
HISTORY_FILE = os.path.join(BASE_DIR, 'upload_history.csv')

for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER, PROCESSED_FOLDER, DROPBOX_FOLDER]:
    os.makedirs(folder, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# =============================================================================
# UPLOAD CONCURRENCY
# =============================================================================
UPLOAD_SEMAPHORE = threading.Semaphore(int(os.environ.get('MAX_CONCURRENT_UPLOADS', 50)))

# =============================================================================
# MEMORY SAFETY
# =============================================================================
MEMORY_HIGH_THRESHOLD = int(os.environ.get('MEMORY_HIGH_THRESHOLD', 85))
MEMORY_CRITICAL_THRESHOLD = int(os.environ.get('MEMORY_CRITICAL_THRESHOLD', 92))
MEMORY_RESUME_THRESHOLD = int(os.environ.get('MEMORY_RESUME_THRESHOLD', 75))
DEMUCS_TIMEOUT_SECONDS = int(os.environ.get('DEMUCS_TIMEOUT_SECONDS', 600))
MEMORY_WATCHDOG_INTERVAL = int(os.environ.get('MEMORY_WATCHDOG_INTERVAL', 15))

memory_throttle_event = threading.Event()
memory_throttle_event.set()

# =============================================================================
# SESSION & TRACKING STATE
# =============================================================================
sessions_status = {}
sessions_lock = Lock()

download_tracker = {}
download_tracker_lock = Lock()

pending_downloads = {}
pending_downloads_lock = Lock()

scheduled_deletions = {}
scheduled_deletions_lock = Lock()

DELETION_DELAY_MINUTES = int(os.environ.get('DELETION_DELAY_MINUTES', 300))
MAX_PENDING_TRACKS = int(os.environ.get('MAX_PENDING_TRACKS', 1500))
PENDING_WARNING_THRESHOLD = int(os.environ.get('PENDING_WARNING_THRESHOLD', 1000))

# Sequential processing
track_download_status = {}
track_download_status_lock = Lock()
current_processing_track = None
current_processing_track_lock = Lock()
SEQUENTIAL_MODE = True

# =============================================================================
# UPLOAD HISTORY STATE
# =============================================================================
upload_history = {}
upload_history_lock = Lock()

# =============================================================================
# DROPBOX CONFIGURATION
# =============================================================================
DROPBOX_ACCESS_TOKEN = os.environ.get('DROPBOX_ACCESS_TOKEN', '')
DROPBOX_REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', '')
DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')
DROPBOX_APP_SECRET = os.environ.get('DROPBOX_APP_SECRET', '')
DROPBOX_TEAM_MEMBER_ID = os.environ.get('DROPBOX_TEAM_MEMBER_ID', '')

dropbox_token_lock = Lock()
dropbox_current_token = DROPBOX_ACCESS_TOKEN
dropbox_token_expires_at = 0

dropbox_imports = {}
dropbox_imports_lock = Lock()

bulk_import_state = {
    'active': False,
    'stop_requested': False,
    'folder_path': '',
    'namespace_id': '',
    'started_at': None,
    'total_found': 0,
    'scanning_found': 0,
    'total_scanned': 0,
    'downloaded': 0,
    'processed': 0,
    'failed': 0,
    'skipped': 0,
    'current_file': '',
    'current_status': 'idle',
    'files_queue': [],
    'completed_files': [],
    'failed_files': [],
    'skipped_files': [],
    'error': None,
    'last_update': None
}
bulk_import_lock = Lock()

# =============================================================================
# API & DATABASE
# =============================================================================
API_ENDPOINT = os.environ.get('API_ENDPOINT', 'https://track.idbyrivoli.com/upload')
API_KEY = os.environ.get('API_KEY', '5X#JP5ifkSm?oE6@haMriYG$j!87BEfX@zg3CxcE')
USE_DATABASE_MODE = os.environ.get('USE_DATABASE_MODE', 'true').lower() in ('true', '1', 'yes')
CURRENT_HOST_URL = os.environ.get('PUBLIC_URL', '')

# Initialize database service if in database mode
_database_service = None
if USE_DATABASE_MODE:
    try:
        from database_service import get_database_service, save_track_to_database, check_database_connection
        _database_service = get_database_service()
        if check_database_connection():
            print("✅ Database mode enabled - tracks will be saved directly to database")
        else:
            print("⚠️ Database mode enabled but connection failed - falling back to API mode")
            USE_DATABASE_MODE = False
    except ImportError as e:
        print(f"⚠️ Database service not available: {e}")
        USE_DATABASE_MODE = False
    except Exception as e:
        print(f"⚠️ Database initialization failed: {e}")
        USE_DATABASE_MODE = False

if not USE_DATABASE_MODE:
    print("📡 API mode enabled - tracks will be sent to external API")

# =============================================================================
# DEMUCS DEVICE
# =============================================================================
FORCE_DEVICE = os.environ.get('DEMUCS_FORCE_DEVICE', '').strip().lower()
DEMUCS_DEVICE = 'cpu'  # Will be set properly during startup

# =============================================================================
# WORKER CONFIGURATION
# =============================================================================
CPU_COUNT = multiprocessing.cpu_count()
NUM_WORKERS = 1  # Will be set properly during startup

# =============================================================================
# QUEUE
# =============================================================================
track_queue = queue.Queue()
queue_items = {}
queue_items_lock = Lock()
MAX_PROCESSING_TIME = 30 * 60

# =============================================================================
# BATCH
# =============================================================================
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 200))
BATCH_MODE_ENABLED = os.environ.get('BATCH_MODE', 'true').lower() == 'true'
batch_processed_count = 0
batch_lock = Lock()

# =============================================================================
# DISK CLEANUP
# =============================================================================
DISK_THRESHOLD_PERCENT = int(os.environ.get('DISK_THRESHOLD_PERCENT', 80))
TRACKS_TO_DELETE = int(os.environ.get('TRACKS_TO_DELETE', 25000))
DISK_CHECK_INTERVAL_SECONDS = int(os.environ.get('DISK_CHECK_INTERVAL', 60))
DISK_CLEANUP_ENABLED = os.environ.get('DISK_CLEANUP', 'true').lower() == 'true'
disk_cleanup_in_progress = False
disk_cleanup_lock = Lock()

# =============================================================================
# DELAYED DELETION
# =============================================================================
DELAYED_DELETE_MINUTES = int(os.environ.get('DELAYED_DELETE_MINUTES', 5))
DELAYED_DELETE_ENABLED = os.environ.get('DELAYED_DELETE', 'true').lower() == 'true'

# =============================================================================
# CLEANUP
# =============================================================================
CLEANUP_ON_START = os.environ.get('CLEANUP_ON_START', 'false').lower() == 'true'
DELETE_AFTER_DOWNLOAD = os.environ.get('DELETE_AFTER_DOWNLOAD', 'false').lower() == 'true'
MAX_FILE_AGE_HOURS = int(os.environ.get('MAX_FILE_AGE_HOURS', 10))
CLEANUP_INTERVAL_MINUTES = int(os.environ.get('CLEANUP_INTERVAL_MINUTES', 500))

# =============================================================================
# RETRY
# =============================================================================
MAX_RETRY_ATTEMPTS = int(os.environ.get('MAX_RETRY_ATTEMPTS', 3))
RETRY_DELAY_SECONDS = int(os.environ.get('RETRY_DELAY_SECONDS', 5))

# =============================================================================
# WORKER THREAD REFERENCES (populated at startup)
# =============================================================================
worker_threads = []
