"""
IDByRivoli Audio Processor - Application Entry Point

Slim entrypoint that:
1. Imports the Flask app and configuration
2. Registers all route blueprints
3. Initializes background threads (workers, watchdog, cleanup)
4. Starts the server
"""
import os
import threading

# =============================================================================
# 1. IMPORT CORE CONFIG (creates Flask app, loads env, patches torchaudio)
# =============================================================================
from config import app, BASE_DIR

# =============================================================================
# 2. REGISTER BLUEPRINTS
# =============================================================================
from routes.main import main_bp
from routes.upload import upload_bp
from routes.dropbox import dropbox_bp
from routes.download import download_bp
from routes.status import status_bp
from routes.history import history_bp
from routes.cleanup import cleanup_bp

app.register_blueprint(main_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(dropbox_bp)
app.register_blueprint(download_bp)
app.register_blueprint(status_bp)
app.register_blueprint(history_bp)
app.register_blueprint(cleanup_bp)

# =============================================================================
# 3. INITIALIZE SERVICES & LOAD PERSISTED STATE
# =============================================================================
from utils.history import load_history_from_csv
from utils.tracking import process_scheduled_deletions

import config as cfg
from services.track_service import get_demucs_device, get_optimal_workers
from services.queue_service import log_message

# Detect GPU/CPU device for Demucs
cfg.DEMUCS_DEVICE = get_demucs_device()

# Calculate optimal workers based on hardware
cfg.NUM_WORKERS = get_optimal_workers()
print(f"🔧 Configuration: {cfg.CPU_COUNT} CPUs détectés → {cfg.NUM_WORKERS} workers parallèles")

# Load persisted upload history from CSV
load_history_from_csv()

# Print title filter configuration
from services.metadata_service import SKIP_KEYWORDS, DJ_NAMES_TO_REPLACE
print(f"🏷️  Title Cleaning: {len(SKIP_KEYWORDS)} skip keywords, {len(DJ_NAMES_TO_REPLACE)} DJ names to replace")

# =============================================================================
# 4. START BACKGROUND THREADS
# =============================================================================

# --- Scheduled deletion thread ---
scheduled_deletion_thread = threading.Thread(
    target=process_scheduled_deletions, daemon=True
)
scheduled_deletion_thread.start()
print("📥 Pending downloads system initialized (files stay until API confirms download)")

# --- Worker threads ---
from services.track_service import worker

cfg.worker_threads = []
for i in range(cfg.NUM_WORKERS):
    t = threading.Thread(target=worker, args=(i + 1,), daemon=True)
    t.start()
    cfg.worker_threads.append(t)
print(f"🚀 {cfg.NUM_WORKERS} workers démarrés")

# --- Memory watchdog ---
from services.memory_service import memory_watchdog

watchdog_thread = threading.Thread(target=memory_watchdog, daemon=True)
watchdog_thread.start()
print(
    f"🛡️ Memory watchdog started (check every {cfg.MEMORY_WATCHDOG_INTERVAL}s, "
    f"high={cfg.MEMORY_HIGH_THRESHOLD}%, critical={cfg.MEMORY_CRITICAL_THRESHOLD}%)"
)

# --- Startup cleanup ---
from services.cleanup_service import startup_cleanup, periodic_cleanup, disk_monitor_loop

startup_cleanup()

# --- Periodic cleanup thread ---
cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
cleanup_thread.start()

# --- Disk monitor thread ---
if cfg.DISK_CLEANUP_ENABLED:
    disk_monitor_thread = threading.Thread(target=disk_monitor_loop, daemon=True)
    disk_monitor_thread.start()
    print(
        f"💾 Disk cleanup: ENABLED (threshold={cfg.DISK_THRESHOLD_PERCENT}%, "
        f"delete={cfg.TRACKS_TO_DELETE} oldest tracks)"
    )
else:
    print("💾 Disk cleanup: DISABLED")

# --- Print storage management summary ---
print(f"")
print(f"🔧 ════════════════════════════════════════════════")
print(f"🔧 STORAGE MANAGEMENT SETTINGS:")
print(f"   CLEANUP_ON_START: {cfg.CLEANUP_ON_START}")
print(f"   DELETE_AFTER_DOWNLOAD: {cfg.DELETE_AFTER_DOWNLOAD}")
print(f"   DELETION_DELAY_MINUTES: {cfg.DELETION_DELAY_MINUTES}min (after /confirm_download)")
print(f"   MAX_FILE_AGE_HOURS: {cfg.MAX_FILE_AGE_HOURS}h (periodic cleanup)")
print(f"   CLEANUP_INTERVAL_MINUTES: {cfg.CLEANUP_INTERVAL_MINUTES}min")
print(f"🔧 ════════════════════════════════════════════════")
print(f"")
print(f"📦 Batch tracking: {'ENABLED' if cfg.BATCH_MODE_ENABLED else 'DISABLED'} "
      f"(milestone every {cfg.BATCH_SIZE} tracks, no pause)")
print(f"⏰ Delayed delete: {'ENABLED' if cfg.DELAYED_DELETE_ENABLED else 'DISABLED'} "
      f"({cfg.DELAYED_DELETE_MINUTES}min after download)")

# --- Auto-resume interrupted bulk import ---
from routes.dropbox import auto_resume_bulk_import
auto_resume_bulk_import()
    
    # =============================================================================
# 5. HELPER (kill jupyter before start)
    # =============================================================================

def kill_jupyter():
    """Kill any running Jupyter processes to free up resources."""
    try:
        import signal
        import subprocess
        result = subprocess.run(['pgrep', '-f', 'jupyter'], capture_output=True, text=True)
        pids = result.stdout.strip().split('\n')
        killed = 0
        for pid in pids:
            if pid:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    killed += 1
                except (ProcessLookupError, ValueError):
                    pass
        if killed > 0:
            print(f"🔪 Killed {killed} Jupyter process(es)")
    except Exception as e:
        print(f"⚠️ Could not kill Jupyter: {e}")


# =============================================================================
# 6. MAIN
# =============================================================================

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ID By Rivoli Audio Processor')
    parser.add_argument(
        '-p', '--port', type=int,
        default=int(os.environ.get('PORT', 8888)),
        help='Port to run the server on (default: 8888)',
    )
    parser.add_argument(
        '--debug', action='store_true', default=False,
        help='Enable debug mode (development only)',
    )
    parser.add_argument(
        '--dev', action='store_true', default=False,
        help='Run in development mode with Flask dev server',
    )
    args = parser.parse_args()
    
    kill_jupyter()
    
    if args.dev or args.debug:
        print(f"🔧 Starting ID By Rivoli in DEVELOPMENT mode on port {args.port}")
        app.run(host='0.0.0.0', port=args.port, debug=True)
    else:
        print(f"🚀 Starting ID By Rivoli in PRODUCTION mode on port {args.port}")
        print(f"💡 Tip: For better performance, use: gunicorn -c gunicorn_config.py app:app")
        app.run(host='0.0.0.0', port=args.port, debug=False, threaded=True)
