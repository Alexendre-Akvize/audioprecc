#!/bin/bash
# =============================================================================
# ID By Rivoli - Production Startup Script
# OPTIMIZED FOR: H100 80GB | 20 vCPU | 240GB RAM | 5TB Scratch
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║      ID By Rivoli - MAXIMUM THROUGHPUT Production Server     ║${NC}"
echo -e "${CYAN}║      H100 80GB | 20 vCPU | 240GB RAM | 5TB Scratch           ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

# Configuration - OPTIMIZED FOR H100 + 240GB RAM
PORT=${PORT:-8888}
WORKERS=${GUNICORN_WORKERS:-6}        # More Gunicorn workers for high RAM
TIMEOUT=${GUNICORN_TIMEOUT:-600}      # 10 min timeout for batch uploads

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create required directories
echo -e "${YELLOW}📁 Creating directories...${NC}"
mkdir -p uploads output processed static/covers

# Check GPU availability
echo -e "${YELLOW}🔍 Checking GPU...${NC}"
if command -v nvidia-smi &> /dev/null; then
    GPU_INFO=$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits 2>/dev/null || echo "")
    if [ -n "$GPU_INFO" ]; then
        echo -e "${GREEN}✓ GPU detected: $GPU_INFO${NC}"
        
        # Check if it's an H100
        if echo "$GPU_INFO" | grep -qi "H100"; then
            echo -e "${GREEN}🚀 NVIDIA H100 detected - Maximum performance mode${NC}"
            # H100 can handle more concurrent Demucs processes
            export CUDA_VISIBLE_DEVICES=0
        fi
    else
        echo -e "${YELLOW}⚠ No GPU detected, using CPU mode${NC}"
    fi
else
    echo -e "${YELLOW}⚠ nvidia-smi not found, GPU status unknown${NC}"
fi

# Kill any existing processes on the port
echo -e "${YELLOW}🔄 Checking for existing processes on port $PORT...${NC}"
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${YELLOW}Killing existing process on port $PORT...${NC}"
    kill -9 $(lsof -Pi :$PORT -sTCP:LISTEN -t) 2>/dev/null || true
    sleep 2
fi

# Kill any zombie Demucs/Python processes
echo -e "${YELLOW}🧹 Cleaning up zombie processes...${NC}"
pkill -f "jupyter" 2>/dev/null || true
pkill -f "demucs.*--device" 2>/dev/null || true

# Clear CUDA cache
echo -e "${YELLOW}🧹 Clearing CUDA cache...${NC}"
python3 -c "import torch; torch.cuda.empty_cache()" 2>/dev/null || true

# Set optimized environment variables for H100 + 240GB RAM
export PYTORCH_CUDA_ALLOC_CONF="max_split_size_mb:1024,expandable_segments:True"
export OMP_NUM_THREADS=${OMP_NUM_THREADS:-20}
export MKL_NUM_THREADS=${MKL_NUM_THREADS:-20}
export NUMEXPR_MAX_THREADS=${NUMEXPR_MAX_THREADS:-20}

# For H100: Enable TensorFloat-32 for faster computation
export NVIDIA_TF32_OVERRIDE=1
export TORCH_ALLOW_TF32_CUBLAS_OVERRIDE=1

# Optimize for high RAM system
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_MMAP_MAX_=65536

# Maximize file descriptors for high concurrency
ulimit -n 65535 2>/dev/null || true

echo -e "${GREEN}⚡ Environment optimized for H100 + 240GB RAM${NC}"

echo ""
echo -e "${GREEN}🚀 Starting Gunicorn Production Server...${NC}"
echo -e "${BLUE}   Port: $PORT${NC}"
echo -e "${BLUE}   Workers: $WORKERS${NC}"
echo -e "${BLUE}   Timeout: ${TIMEOUT}s${NC}"
echo ""

# Start with Gunicorn for production
exec gunicorn \
    --config gunicorn_config.py \
    --bind "0.0.0.0:$PORT" \
    --workers "$WORKERS" \
    --timeout "$TIMEOUT" \
    --access-logfile - \
    --error-logfile - \
    --capture-output \
    app:app
