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

# Proxy configuration
HUB_PROXY_PORT=${HUB_PROXY_PORT:-15252}
HUB_REAL_PORT=${HUB_PORT:-5252}

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
    
    # Create proxy - proxy name is the last positional argument
    $cli -h "localhost:$TOXIPROXY_PORT" create \
        -l "localhost:$HUB_PROXY_PORT" \
        -u "localhost:$HUB_REAL_PORT" \
        hub_proxy 2>/dev/null || true
    
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
