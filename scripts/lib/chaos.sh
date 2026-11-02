#!/bin/bash
# scripts/lib/chaos.sh - Toxiproxy chaos engineering for network simulation
#
# This library provides functions to simulate network issues using Toxiproxy:
# - Latency injection
# - Connection resets
# - Bandwidth limitations
# - Timeout simulation

TOXIPROXY_PORT=${TOXIPROXY_PORT:-8474}
TOXIPROXY_PID=""
CHAOS_INJECTION_PID=""

# Proxy configuration. Behind a hub of its own the proxy sits in front of HUB_PORT; a scenario whose
# clients reach the hubs through another front (the TestProxy of the HA setup) names its port.
HUB_PROXY_PORT=${HUB_PROXY_PORT:-15252}
HUB_REAL_PORT=${CHAOS_UPSTREAM_PORT:-${HUB_PORT:-5252}}

# Start Toxiproxy server
start_toxiproxy() {
    log_step "Starting Toxiproxy on port $TOXIPROXY_PORT..."
    
    # Kill any existing toxiproxy
    pkill -f "toxiproxy-server" 2>/dev/null || true
    sleep 1
    
    # Start toxiproxy server
    "$PROJECT_ROOT/tools/toxiproxy-server" -port $TOXIPROXY_PORT > "${OUTPUT_DIR}/toxiproxy.log" 2>&1 &
    TOXIPROXY_PID=$!
    register_pid $TOXIPROXY_PID
    
    # Wait for toxiproxy to be ready
    local retries=0
    while ! curl -s "http://localhost:$TOXIPROXY_PORT/version" > /dev/null 2>&1; do
        sleep 0.5
        retries=$((retries + 1))
        if [ $retries -gt 20 ]; then
            log_error "Toxiproxy failed to start"
            return 1
        fi
    done
    
    log_info "Toxiproxy started (PID: $TOXIPROXY_PID)"
}

# Create proxy for the Hub
create_hub_proxy() {
    log_step "Creating Hub proxy (localhost:$HUB_PROXY_PORT -> localhost:$HUB_REAL_PORT)..."
    
    local cli="$PROJECT_ROOT/tools/toxiproxy-cli"
    
    # Create proxy - proxy name is the last positional argument. A failed create is tolerated only
    # if the proxy is there afterwards (a leftover from an earlier run makes it fail); the check
    # below is what decides, so the CLI's own message is kept for when it does not pass.
    local create_output
    create_output=$("$cli" -h "localhost:$TOXIPROXY_PORT" create \
        -l "localhost:$HUB_PROXY_PORT" \
        -u "localhost:$HUB_REAL_PORT" \
        hub_proxy 2>&1) || true

    if ! curl -sf "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy" > /dev/null; then
        log_error "Hub proxy does not exist after creating it. toxiproxy-cli said: ${create_output:-<nothing>}"
        return 1
    fi

    log_info "Hub proxy created"
    
    # Apply permanent bandwidth limit (100 Mbps = 12.5 MB/s = 12500 KB/s)
    inject_bandwidth_limit
}

# Inject bandwidth limit (100 Mbps = 12500 KB/s)
inject_bandwidth_limit() {
    local rate_kbps=${1:-12500}  # 100 Mbps default
    
    curl -s -X POST "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"bandwidth_limit\",\"type\":\"bandwidth\",\"attributes\":{\"rate\":$rate_kbps}}" \
        > /dev/null 2>&1 || true
    
    log_info "Applied bandwidth limit: $((rate_kbps * 8 / 1000)) Mbps ($rate_kbps KB/s)"
}

# Inject latency toxic via REST API
inject_latency() {
    local latency_ms=${1:-500}
    local jitter_ms=${2:-200}
    
    curl -s -X POST "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"chaos_latency\",\"type\":\"latency\",\"attributes\":{\"latency\":$latency_ms,\"jitter\":$jitter_ms}}" \
        > /dev/null 2>&1 || true
    
    log_info "Injected latency: ${latency_ms}ms ± ${jitter_ms}ms"
}

# Remove latency toxic
remove_latency() {
    curl -s -X DELETE "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics/chaos_latency" \
        > /dev/null 2>&1 || true
}

# Inject connection reset
inject_reset() {
    curl -s -X POST "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics" \
        -H "Content-Type: application/json" \
        -d '{"name":"chaos_reset","type":"reset_peer","attributes":{"timeout":0}}' \
        > /dev/null 2>&1 || true
    
    log_info "Injected connection reset"
}

# Remove connection reset
remove_reset() {
    curl -s -X DELETE "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics/chaos_reset" \
        > /dev/null 2>&1 || true
}

# Inject timeout (slow close)
inject_timeout() {
    local delay_ms=${1:-5000}
    
    curl -s -X POST "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"chaos_timeout\",\"type\":\"timeout\",\"attributes\":{\"timeout\":$delay_ms}}" \
        > /dev/null 2>&1 || true
    
    log_info "Injected timeout: ${delay_ms}ms"
}

# Remove timeout
remove_timeout() {
    curl -s -X DELETE "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics/chaos_timeout" \
        > /dev/null 2>&1 || true
}

# Hold every byte in both directions, refusing nothing: a cable pulled and not yet replugged as
# each end sees it. `inject_timeout` cannot stand in for this - Toxiproxy applies a toxic to the
# downstream only unless told otherwise, so the clients' uploads would keep flowing.
# Non-zero when a toxic was not created, because a fault that never happened would let the
# scenario pass for the wrong reason.
inject_silent_hold() {
    local stream
    for stream in downstream upstream; do
        curl -sf -X POST "http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy/toxics" \
            -H "Content-Type: application/json" \
            -d "{\"name\":\"chaos_hold_$stream\",\"type\":\"timeout\",\"stream\":\"$stream\",\"attributes\":{\"timeout\":0}}" \
            > /dev/null || return 1
    done
    log_info "Holding the hub proxy silent"
}

# End the hold by closing every connection that went through it. Removing the toxics alone would
# hand the held bytes on to connections the clients may be about to give up on, which is a
# different fault: the one being reproduced is a link that comes back with its connections closed.
# The proxy is disabled while the toxics are still attached so that nothing is delivered on the
# way out, and enabled again once they are gone.
release_silent_hold() {
    local base="http://localhost:$TOXIPROXY_PORT/proxies/hub_proxy"
    curl -sf -X POST "$base" -H "Content-Type: application/json" -d '{"enabled":false}' > /dev/null || return 1
    curl -s -X DELETE "$base/toxics/chaos_hold_downstream" > /dev/null || true
    curl -s -X DELETE "$base/toxics/chaos_hold_upstream" > /dev/null || true
    curl -sf -X POST "$base" -H "Content-Type: application/json" -d '{"enabled":true}' > /dev/null || return 1
    log_info "Released the hub proxy, connections closed"
}

# One fault instead of a loop. Started once the sender is running, so that the delay is counted
# from the transfer and not from the setup. Each moment is written to chaos-events.log together
# with whether the sender was still alive, which is what lets the runner tell a fault that landed
# mid-transfer from one that fired after the transfer was over.
start_hold_injection() {
    local start_after=$1 hold_for=$2
    local events="${OUTPUT_DIR:-output}/chaos-events.log"
    : > "$events"

    log_step "Scheduling one silent hold of ${hold_for}s, ${start_after}s after the sender started..."
    (
        sleep "$start_after"
        if inject_silent_hold; then
            echo "held sender_alive=$(pgrep -f 'Sender.dll' > /dev/null && echo yes || echo no) at=$(date +%s)" >> "$events"
        else
            echo "hold_failed at=$(date +%s)" >> "$events"
            exit 1
        fi
        sleep "$hold_for"
        if release_silent_hold; then
            echo "released sender_alive=$(pgrep -f 'Sender.dll' > /dev/null && echo yes || echo no) at=$(date +%s)" >> "$events"
        else
            echo "release_failed at=$(date +%s)" >> "$events"
        fi
    ) &

    CHAOS_INJECTION_PID=$!
    register_pid $CHAOS_INJECTION_PID
}

# Run periodic chaos injection in background
start_chaos_injection() {
    log_step "Starting chaos injection loop..."
    
    (
        sleep 5  # Let transfer start
        
        while true; do
            # Phase 1: Jittery Latency
            inject_latency 200 50
            sleep 5
            remove_latency
            
            # Phase 2: Connection Reset (Aggressive)
            inject_reset
            sleep 1
            remove_reset
            
            # Short Recovery
            sleep 10
            
            # Phase 3: Another Connection Reset
            inject_reset
            sleep 1
            remove_reset

            # Phase 4: High Latency
            inject_latency 300 100
            sleep 5
            remove_latency
            
            # Phase 5: Connection Reset
            inject_reset
            sleep 1
            remove_reset

            # Recovery before next loop
            sleep 5
        done
    ) &
    
    CHAOS_INJECTION_PID=$!
    register_pid $CHAOS_INJECTION_PID
    log_info "Chaos injection started (PID: $CHAOS_INJECTION_PID)"
}

# Stop chaos injection
stop_chaos_injection() {
    if [ -n "$CHAOS_INJECTION_PID" ]; then
        kill $CHAOS_INJECTION_PID 2>/dev/null || true
    fi
}

# Stop Toxiproxy
stop_toxiproxy() {
    stop_chaos_injection
    if [ -n "$TOXIPROXY_PID" ]; then
        kill $TOXIPROXY_PID 2>/dev/null || true
    fi
    pkill -f "toxiproxy-server" 2>/dev/null || true
}
