#!/bin/bash
# scripts/lib/process.sh - Process management for benchmark scripts

# Arrays to track PIDs for cleanup
declare -a MANAGED_PIDS=()
declare -a RECEIVER_PIDS=()
SENDER_PID=""


# Register a PID for cleanup
register_pid() {
    MANAGED_PIDS+=("$1")
}

# Cleanup all registered processes
cleanup_processes() {
    log_step "Stopping processes..."
    for pid in "${MANAGED_PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    
    # Force kill remaining processes
    pkill -f "DataHub.dll" 2>/dev/null || true
    pkill -f "Sender.dll" 2>/dev/null || true
    pkill -f "Receiver.dll" 2>/dev/null || true
}

# Start DataHub
start_hub() {
    local hub_port=${HUB_PORT:-5252}
    
    ensure_port_free $hub_port || return 1
    
    log_step "Starting DataHub..."
    USE_REDIS="$USE_REDIS" \
    ASPNETCORE_URLS="http://localhost:$hub_port" \
        dotnet samples/DataHub/bin/Release/net10.0/DataHub.dll &
    local hub_pid=$!
    register_pid $hub_pid
    
    wait_for_url "http://localhost:$hub_port/health" "Hub" || return 1
    echo $hub_pid
}

# Start Receiver(s)
start_receivers() {
    local count=${1:-1}
    
    for ((i=1; i<=count; i++)); do
        log_step "Starting Receiver $i..."
        dotnet samples/Receiver/bin/Release/net10.0/Receiver.dll > "${OUTPUT_DIR:-output}/receiver_$i.log" 2>&1 &
        local pid=$!
        register_pid $pid
        RECEIVER_PIDS+=("$pid")  # Track separately for waiting
    done
    sleep 2  # Let receivers initialize
}

# Start Sender
start_sender() {
    log_step "Starting Sender..."
    dotnet samples/Sender/bin/Release/net10.0/Sender.dll &
    local sender_pid=$!
    register_pid $sender_pid
    SENDER_PID=$sender_pid  # global, so a caller can sample it without capturing stdout
    echo $sender_pid
}

# Wait for receivers to complete. Without TRANSFER_TIMEOUT_SECONDS there is no deadline; with it,
# a receiver still running when it expires is a failure and the function returns non-zero.
wait_for_receivers() {
    log_step "Waiting for transfer to complete..."

    local deadline=""
    if [ -n "$TRANSFER_TIMEOUT_SECONDS" ]; then
        deadline=$(( $(date +%s) + TRANSFER_TIMEOUT_SECONDS ))
    fi

    for pid in "${RECEIVER_PIDS[@]}"; do
        if ! ps -p $pid > /dev/null 2>&1; then
            continue
        fi
        if [ -z "$deadline" ]; then
            wait $pid 2>/dev/null || true
            continue
        fi
        while ps -p $pid > /dev/null 2>&1; do
            if [ "$(date +%s)" -ge "$deadline" ]; then
                log_error "❌ Error: receiver $pid still running ${TRANSFER_TIMEOUT_SECONDS}s after the sender started"
                return 1
            fi
            sleep 1
        done
        wait $pid 2>/dev/null || true
    done
}

SENDER_EXIT_WAIT_SECONDS=30

wait_for_sender() {
    if [ -z "$SENDER_PID" ]; then
        return 0
    fi

    local waited=0
    while ps -p $SENDER_PID > /dev/null 2>&1; do
        if [ "$waited" -ge "$SENDER_EXIT_WAIT_SECONDS" ]; then
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    wait $SENDER_PID 2>/dev/null || true
}
