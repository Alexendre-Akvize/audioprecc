"""
Memory management service for IDByRivoli.

Monitors RAM usage, triggers garbage collection, and prevents OOM crashes.
"""
import gc
import time
import psutil

from config import (
    MEMORY_HIGH_THRESHOLD,
    MEMORY_CRITICAL_THRESHOLD,
    MEMORY_RESUME_THRESHOLD,
    MEMORY_WATCHDOG_INTERVAL,
)


def get_memory_percent():
    """Get current RAM usage percentage."""
    try:
        return psutil.virtual_memory().percent
    except:
        return 0


def force_garbage_collect(reason=""):
    """Force aggressive garbage collection to free memory."""
    try:
        collected = gc.collect(generation=2)  # Full collection
        if reason:
            print(f"🧹 GC ({reason}): collected {collected} objects, RAM: {get_memory_percent():.1f}%")
    except:
        pass


def wait_for_memory_available(worker_id=0, timeout=300):
    """
    Block the worker until memory drops below the high threshold.
    Returns True if memory is available, False if timed out.
    """
    mem = get_memory_percent()
    if mem < MEMORY_HIGH_THRESHOLD:
        return True
    
    print(f"⚠️ Worker {worker_id}: RAM at {mem:.1f}% >= {MEMORY_HIGH_THRESHOLD}% - PAUSING until memory frees up")
    force_garbage_collect(f"Worker {worker_id} memory pressure")
    
    # Wait with exponential backoff
    wait_time = 2
    total_waited = 0
    while total_waited < timeout:
        time.sleep(wait_time)
        total_waited += wait_time
        mem = get_memory_percent()
        
        if mem >= MEMORY_CRITICAL_THRESHOLD:
            print(f"🔴 Worker {worker_id}: CRITICAL RAM {mem:.1f}% - forcing aggressive GC")
            force_garbage_collect(f"CRITICAL Worker {worker_id}")
        
        if mem < MEMORY_RESUME_THRESHOLD:
            print(f"✅ Worker {worker_id}: RAM dropped to {mem:.1f}% - resuming")
            return True
        
        wait_time = min(wait_time * 1.5, 30)  # Max 30s between checks
        print(f"⏳ Worker {worker_id}: Still waiting... RAM: {mem:.1f}% (waited {total_waited:.0f}s/{timeout}s)")
    
    print(f"⚠️ Worker {worker_id}: Memory timeout after {timeout}s, RAM still at {mem:.1f}% - proceeding anyway")
    return False


def memory_watchdog():
    """Background thread that monitors memory and forces cleanup when needed."""
    consecutive_high = 0
    while True:
        try:
            time.sleep(MEMORY_WATCHDOG_INTERVAL)
            mem = get_memory_percent()
            
            if mem >= MEMORY_CRITICAL_THRESHOLD:
                consecutive_high += 1
                print(f"🔴 MEMORY WATCHDOG: CRITICAL {mem:.1f}% (consecutive: {consecutive_high})")
                
                # Force aggressive garbage collection
                force_garbage_collect("WATCHDOG CRITICAL")
                
                # If memory stays critical for 3+ checks, try to free torch cache
                if consecutive_high >= 3:
                    try:
                        import torch
                        if torch.cuda.is_available():
                            torch.cuda.empty_cache()
                            print(f"🔴 WATCHDOG: Cleared CUDA cache")
                    except:
                        pass
                        
            elif mem >= MEMORY_HIGH_THRESHOLD:
                consecutive_high += 1
                if consecutive_high % 4 == 0:  # Log every 4th check to avoid spam
                    print(f"⚠️ MEMORY WATCHDOG: HIGH {mem:.1f}% (consecutive: {consecutive_high})")
                force_garbage_collect("WATCHDOG HIGH")
            else:
                if consecutive_high > 0:
                    print(f"✅ MEMORY WATCHDOG: Recovered to {mem:.1f}% (was high for {consecutive_high} checks)")
                consecutive_high = 0
                
        except Exception as e:
            print(f"⚠️ Memory watchdog error: {e}")
