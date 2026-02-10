"""
Gunicorn Production Configuration for ID By Rivoli
═══════════════════════════════════════════════════════════════════
OPTIMIZED FOR: H100 80GB VRAM | 20 vCPU | 240GB RAM | 5TB Scratch
═══════════════════════════════════════════════════════════════════
"""

import multiprocessing
import os

# Server Socket
bind = "0.0.0.0:8888"
backlog = 4096  # Increased for high concurrency

# Worker Processes
# IMPORTANT: For GPU workloads, use 1 worker to avoid CUDA memory conflicts
# The actual parallelism happens in background threads within this single process
workers = 1
worker_class = "gthread"  # threaded worker for handling concurrent HTTP requests
threads = 16  # Many threads for concurrent uploads/downloads

# Worker timeout - increased for 1000+ track batch uploads
timeout = 1800  # 30 minutes for very large batches (1000+ tracks)
graceful_timeout = 120
keepalive = 30

# Request limits - DISABLED for single-worker GPU setup
# With only 1 worker, max_requests kills ALL background threads (bulk import, 
# GPU workers, etc.) when recycling. Memory management is handled by the 
# app's own memory watchdog instead.
max_requests = 0  # 0 = disabled (never auto-restart)
max_requests_jitter = 0

# Logging
accesslog = "-"  # stdout
errorlog = "-"   # stderr
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = "idbyrivoli"

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# SSL (uncomment if needed)
# keyfile = "/path/to/key.pem"
# certfile = "/path/to/cert.pem"

# IMPORTANT: Don't preload - we need threads to start in the worker process
preload_app = False

# Hooks
def on_starting(server):
    print("🚀 ID By Rivoli Production Server Starting...")

def on_exit(server):
    print("👋 ID By Rivoli Server Shutting Down...")

def worker_exit(server, worker):
    """Called when a worker exits. Log a warning if a bulk import was in progress."""
    print(f"⚠️ Worker {worker.pid} exiting.")
    print(f"   If a bulk import was running, it will auto-resume when the new worker boots.")

def worker_int(worker):
    print(f"Worker {worker.pid} received INT signal")

def worker_abort(worker):
    print(f"Worker {worker.pid} received SIGABRT signal")
