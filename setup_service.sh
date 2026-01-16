#!/bin/bash
# =============================================================================
# ID By Rivoli - Service Installation Script
# Run this once to set up the systemd service
# =============================================================================

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║         ID By Rivoli - Service Installation                  ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${YELLOW}Please run as root (sudo ./setup_service.sh)${NC}"
    exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${YELLOW}Installing service from: $SCRIPT_DIR${NC}"

# Update the service file with the correct path
sed "s|/root/IDByRivoli-separate-audio|$SCRIPT_DIR|g" "$SCRIPT_DIR/idbyrivoli.service" > /etc/systemd/system/idbyrivoli.service

# Find gunicorn path
GUNICORN_PATH=$(which gunicorn 2>/dev/null || echo "/usr/local/bin/gunicorn")
sed -i "s|/usr/local/bin/gunicorn|$GUNICORN_PATH|g" /etc/systemd/system/idbyrivoli.service

echo -e "${GREEN}✓ Service file installed${NC}"

# Reload systemd
systemctl daemon-reload
echo -e "${GREEN}✓ Systemd reloaded${NC}"

# Enable service to start on boot
systemctl enable idbyrivoli
echo -e "${GREEN}✓ Service enabled (will start on boot)${NC}"

# Stop any existing processes
pkill -f gunicorn 2>/dev/null || true
sleep 2

# Start the service
systemctl start idbyrivoli
echo -e "${GREEN}✓ Service started${NC}"

# Show status
echo ""
echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
systemctl status idbyrivoli --no-pager
echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"

echo ""
echo -e "${GREEN}Installation complete!${NC}"
echo ""
echo -e "Commands to manage the service:"
echo -e "  ${CYAN}systemctl status idbyrivoli${NC}   - Check status"
echo -e "  ${CYAN}systemctl stop idbyrivoli${NC}     - Stop server"
echo -e "  ${CYAN}systemctl start idbyrivoli${NC}    - Start server"
echo -e "  ${CYAN}systemctl restart idbyrivoli${NC}  - Restart server"
echo -e "  ${CYAN}journalctl -u idbyrivoli -f${NC}   - View live logs"
echo -e "  ${CYAN}journalctl -u idbyrivoli -n 100${NC} - View last 100 log lines"
echo ""
