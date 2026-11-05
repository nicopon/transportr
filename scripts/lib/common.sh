#!/bin/bash
# scripts/lib/common.sh - Common utilities for benchmark scripts

# Colors
export GREEN='\033[0;32m'
export BLUE='\033[0;34m'
export YELLOW='\033[1;33m'
export RED='\033[0;31m'
export NC='\033[0m'

# Log functions
log_info() { echo -e "${GREEN}$1${NC}"; }
log_warn() { echo -e "${YELLOW}$1${NC}"; }
log_error() { echo -e "${RED}$1${NC}"; }
log_step() { echo -e "${YELLOW}$1${NC}"; }

# True when a container runtime is usable right now. `docker info` rather than
# `command -v docker`, because the CLI being installed says nothing about the
# daemon answering.
docker_available() {
    command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

# Wait for a URL to respond (health check)
wait_for_url() {
    local url=$1
    local name=${2:-"service"}
    local max_attempts=${3:-30}
    
    echo -n "Waiting for $name..."
    for ((i=1; i<=max_attempts; i++)); do
        if curl -s -m 1 "$url" > /dev/null 2>&1; then
            echo -e " ${GREEN}OK${NC}"
            return 0
        fi
        echo -n "."
        sleep 0.5
    done
    echo -e " ${RED}FAILED${NC}"
    return 1
}

# Ensure port is free
ensure_port_free() {
    local port=$1
    local max_attempts=${2:-10}
    
    for ((i=1; i<=max_attempts; i++)); do
        local pids=$(lsof -ti:$port 2>/dev/null)
        if [ -z "$pids" ]; then
            return 0
        fi
        log_warn "Port $port in use by PID(s): $pids. Killing..."
        kill -9 $pids 2>/dev/null
        sleep 1
    done
    
    log_error "Unable to free port $port"
    return 1
}

# Generate test data if needed
ensure_test_data() {
    local item_count=${1:-2000000}
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    if [ ! -f "data/orders.bin" ]; then
        log_step "Generating $item_count test items..."
        ITEM_COUNT=$item_count "$script_dir/init.sh" > /dev/null
    fi
}

LOG_LINE_WAIT_SECONDS=10

# A line another process has just written can reach a reader late on a shared volume, and a
# scenario's output can sit on one. So an absent line is waited for before it is called absent.
# Prints the lines of "$OUTPUT_DIR"/*.log matching the grep pattern $1 as soon as they hold $2
# distinct keys (default 1), a line's key being what the sed expression $3 makes of it (default: the
# line itself). Returns non-zero, still printing what it found, when the window closes first.
wait_for_log_lines() {
    local pattern=$1
    local wanted=${2:-1}
    local key=${3:-}
    local polls=$(( LOG_LINE_WAIT_SECONDS * 2 ))
    local lines=""
    local i

    for ((i=0; i<=polls; i++)); do
        lines=$(grep -h "$pattern" "$OUTPUT_DIR"/*.log 2>/dev/null || true)
        if [ -n "$lines" ] && [ "$(echo "$lines" | sed -E "$key" | sort -u | wc -l | tr -d ' ')" -ge "$wanted" ]; then
            echo "$lines"
            return 0
        fi
        if [ "$i" -lt "$polls" ]; then sleep 0.5; fi
    done

    if [ -n "$lines" ]; then echo "$lines"; fi
    return 1
}
