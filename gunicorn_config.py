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

# Worker timeout - increase for large file uploads and batch processing
timeout = 600  # 10 minutes for very large batches
graceful_timeout = 60
keepalive = 10

# Request limits - higher for high-RAM system
max_requests = 5000  # More requests before recycling
max_requests_jitter = 100

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

def worker_int(worker):
    print(f"Worker {worker.pid} received INT signal")

def worker_abort(worker):
    print(f"Worker {worker.pid} received SIGABRT signal")
