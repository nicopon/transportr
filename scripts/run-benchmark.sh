#!/bin/bash
# scripts/run-benchmark.sh - Unified benchmark runner
#
# Usage: ./scripts/run-benchmark.sh [options] [scenario]
#
# Options:
#   --no-build   Skip solution build (use when already built)
#
# Scenarios:
#   default  - Dev mode (no auth)
#   oidc     - OIDC with TestIdp
#   data-plane-probe - OIDC, plus refusal checks against the live stream routes
#   chaos    - Network chaos simulation
#   ha-redis - Redis State Store (no chaos)
#
# Examples:
#   ./scripts/run-benchmark.sh              # runs default
#   ./scripts/run-benchmark.sh oidc         # runs OIDC scenario
#   ./scripts/run-benchmark.sh --no-build oidc  # skip build

set -e

# Determine script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Load libraries
source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/process.sh"
source "$SCRIPT_DIR/lib/memory.sh"

# Parse arguments
SKIP_BUILD=false
SCENARIO="default"

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-build)
            SKIP_BUILD=true
            shift
            ;;
        *)
            SCENARIO="$1"
            shift
            ;;
    esac
done

SCENARIO_FILE="$SCRIPT_DIR/scenarios/${SCENARIO}.env"

if [ ! -f "$SCENARIO_FILE" ]; then
    log_error "Unknown scenario: $SCENARIO"
    log_info "Available scenarios:"
    ls -1 "$SCRIPT_DIR/scenarios/" | sed 's/\.env$//'
    exit 1
fi

# Load scenario configuration
source "$SCENARIO_FILE"

# Setup cleanup trap
cleanup() {
    cleanup_processes
    if [ "$ENABLE_IDP" == "true" ]; then
        stop_idp 2>/dev/null || true
    fi
    if [ "$ENABLE_CHAOS" == "true" ]; then
        stop_toxiproxy 2>/dev/null || true
    fi
    if [ -n "$USE_REDIS" ]; then
        stop_redis 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Banner
echo -e "${BLUE}=================================================="
echo "$SCENARIO_NAME"
echo "  Expected: $SCENARIO_EXPECTED"
echo "  Senders : $NUM_SENDERS"
echo "  Receivers: $NUM_RECEIVERS"
echo -e "==================================================${NC}"

# Prepare output directory
export OUTPUT_DIR="${OUTPUT_DIR:-output}"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Build solution (unless --no-build is set)
if [ "$SKIP_BUILD" = "false" ]; then
    log_step "Building solution (Release)..."
    "$SCRIPT_DIR/build_release.sh" > /dev/null
else
    log_step "Skipping build (--no-build)"
fi

# Start IDP if needed (external IDP only)
if [ "$ENABLE_IDP" == "true" ]; then
    source "$SCRIPT_DIR/lib/idp.sh"
    start_idp || exit 1
    acquire_tokens || exit 1
    export EnableAuth=true
    export Oidc__Authority="$IDP_URL"
fi

# Embedded IDP mode: Hub hosts its own IDP, clients acquire tokens themselves
if [ "$ENABLE_EMBEDDED_IDP" == "true" ]; then
    export EnableAuth=true
    export EnableEmbeddedIdp=true
fi

# Ensure test data exists
ensure_test_data ${NUM_ITEMS:-2000000}

# ... (omitted lines) ...

TOTAL_LINES=0
EXPECTED_LINES=${NUM_ITEMS:-2000000}

# Start Toxiproxy if chaos mode is enabled
if [ "$ENABLE_CHAOS" == "true" ]; then
    source "$SCRIPT_DIR/lib/chaos.sh"
    start_toxiproxy || exit 1
fi

# Start Redis if needed
if [ -n "$USE_REDIS" ]; then
    source "$SCRIPT_DIR/lib/redis.sh"
    start_redis || exit 1
fi

# Configure environment
export HUB_PORT=${HUB_PORT:-5252}
export HUB_URL="http://localhost:$HUB_PORT"
export HEALTH_URL="http://localhost:$HUB_PORT/health"

# Sender configuration
export Sender__HubUrl="$HUB_URL"
export Sender__EnableAuth="${ENABLE_AUTH:-false}"
export Sender__BatchSize="$BATCH_SIZE"
export Sender__BatchFlushIntervalMs="$BATCH_FLUSH_INTERVAL_MS"
# Only exported when a scenario asks for it; unset means the historical unbounded sender.
if [ -n "$MAX_IN_FLIGHT_BATCHES" ]; then
    export Sender__MaxInFlightBatches="$MAX_IN_FLIGHT_BATCHES"
fi
export Sender__SourceFilePath="$PROJECT_ROOT/data/orders.bin"
export Sender__Group="senders"
export Sender__OutputDirectory="$OUTPUT_DIR"

# Sender Auth (for automatic token acquisition)
if [ -n "$SENDER_AUTH_CLIENTID" ]; then
    export Sender__Auth__TokenEndpoint="http://localhost:$HUB_PORT/connect/token"
    export Sender__Auth__ClientId="$SENDER_AUTH_CLIENTID"
    export Sender__Auth__ClientSecret="$SENDER_AUTH_SECRET"
    export Sender__Auth__Scope="$SENDER_AUTH_SCOPE"
fi

# Only exported when a scenario asks for it; unset means one transfer, opened at once.
if [ -n "$SENDER_PARALLELISM" ]; then
    export SENDER__PARALLELISM="$SENDER_PARALLELISM"
fi
if [ -n "$SENDER_TRANSFER_STAGGER_SECONDS" ]; then
    export Sender__TransferStaggerSeconds="$SENDER_TRANSFER_STAGGER_SECONDS"
fi

# Receiver configuration
export Receiver__HubUrl="$HUB_URL"
export Receiver__EnableAuth="${ENABLE_AUTH:-false}"
export Receiver__BatchSize="$BATCH_SIZE"
export Receiver__OutputDirectory="$OUTPUT_DIR"
export Receiver__Group="receivers"

# Receive buffer capacity and consumer pacing - only exported when a scenario asked for them, so
# every other run keeps the client defaults.
if [ -n "$RECEIVE_MAX_ITEMS" ]; then
    export Receiver__ReceiveMaxItems="$RECEIVE_MAX_ITEMS"
fi
if [ -n "$RECEIVE_MAX_BYTES" ]; then
    export Receiver__ReceiveMaxBytes="$RECEIVE_MAX_BYTES"
fi
if [ -n "$CONSUME_ITEMS_PER_SECOND" ]; then
    export Receiver__ConsumeItemsPerSecond="$CONSUME_ITEMS_PER_SECOND"
fi

# Receiver Auth (for automatic token acquisition)
if [ -n "$RECEIVER_AUTH_CLIENTID" ]; then
    export Receiver__Auth__TokenEndpoint="http://localhost:$HUB_PORT/connect/token"
    export Receiver__Auth__ClientId="$RECEIVER_AUTH_CLIENTID"
    export Receiver__Auth__ClientSecret="$RECEIVER_AUTH_SECRET"
    export Receiver__Auth__Scope="$RECEIVER_AUTH_SCOPE"
fi

# Logging
export Logging__LogLevel__Default="Information"
export Logging__LogLevel__TransportR="Information"
export ShowProgress=true

# Start Infrastructure
if [ "$ENABLE_HA" == "true" ]; then
    log_info "Starting HA Infrastructure (TestProxy + 2 Hubs)..."
    
    # 1. Start Test Proxy
    log_step "Starting TestProxy (5000)..."
    PROXY_LOAD_BALANCING="$PROXY_LOAD_BALANCING" \
    dotnet tests/TransportR.TestProxy/bin/Release/net10.0/TransportR.TestProxy.dll > "$OUTPUT_DIR/proxy.log" 2>&1 &
    PROXY_PID=$!
    register_pid $PROXY_PID
    sleep 2

    # 2. Start Hub A
    log_step "Starting Hub A (5001)..."
    mkdir -p "$OUTPUT_DIR/hub_a"
    USE_TEST_PROXY="http://localhost:5000" HUB_PORT=5001 ASPNETCORE_URLS="http://localhost:5001" OUTPUT_DIR="$OUTPUT_DIR/hub_a" \
    dotnet samples/DataHub/bin/Release/net10.0/DataHub.dll > "$OUTPUT_DIR/hub_a.log" 2>&1 &
    HUB_A_PID=$!
    register_pid $HUB_A_PID

    # 3. Start Hub B
    log_step "Starting Hub B (5002)..."
    mkdir -p "$OUTPUT_DIR/hub_b"
    USE_TEST_PROXY="http://localhost:5000" HUB_PORT=5002 ASPNETCORE_URLS="http://localhost:5002" OUTPUT_DIR="$OUTPUT_DIR/hub_b" \
    dotnet samples/DataHub/bin/Release/net10.0/DataHub.dll > "$OUTPUT_DIR/hub_b.log" 2>&1 &
    HUB_B_PID=$!
    register_pid $HUB_B_PID

    # Wait for startup
    sleep 5
    
    # Override URLs for clients to point to Proxy
    export HUB_URL="http://localhost:5000"
    export Sender__HubUrl="$HUB_URL"
    export Receiver__HubUrl="$HUB_URL"
    export HEALTH_URL="http://localhost:5000/health" # Proxy doesn't have health but we skip check or check hub direct? 
    # Use generic health check on Hub A for readiness
    wait_for_url "http://localhost:5001/health" "Hub A"
else
    # Standard Single Hub Start
    start_hub || exit 1
fi

# Setup chaos proxy after hub is ready
if [ "$ENABLE_CHAOS" == "true" ]; then
    create_hub_proxy || exit 1
    
    # Override client URLs to use proxy. Only the .env files set CHAOS_CLIENTS: a fault that hits
    # one end while the other stays on the direct route is a different fault from one that hits both.
    case "${CHAOS_CLIENTS:-both}" in
        both)     PROXIED_CLIENTS="sender receiver" ;;
        sender)   PROXIED_CLIENTS="sender" ;;
        receiver) PROXIED_CLIENTS="receiver" ;;
        *) log_error "Unknown CHAOS_CLIENTS: $CHAOS_CLIENTS (both, sender or receiver)"; exit 1 ;;
    esac
    for client in $PROXIED_CLIENTS; do
        case "$client" in
            sender)   export Sender__HubUrl="http://localhost:$HUB_PROXY_PORT" ;;
            receiver) export Receiver__HubUrl="http://localhost:$HUB_PROXY_PORT" ;;
        esac
    done
    log_info "Through Toxiproxy (port $HUB_PROXY_PORT): $PROXIED_CLIENTS"

    case "${CHAOS_PROGRAM:-loop}" in
        loop) start_chaos_injection ;;
        hold) ;;  # started below, once the sender is running
        *) log_error "Unknown CHAOS_PROGRAM: $CHAOS_PROGRAM (loop or hold)"; exit 1 ;;
    esac
fi

# Start Receivers
start_receivers "$NUM_RECEIVERS"

if [ "$ASSERT_TRANSFER_AFFINITY" == "true" ]; then
    source "$SCRIPT_DIR/lib/affinity.sh"
    assert_receivers_on_distinct_hubs || exit 1
fi

# Start Sender
start_sender

if [ "$ENABLE_CHAOS" == "true" ] && [ "${CHAOS_PROGRAM:-loop}" == "hold" ]; then
    start_hold_injection "${CHAOS_HOLD_START_SECONDS:?}" "${CHAOS_HOLD_SECONDS:?}"
fi

# Data plane probe: runs against the live transfer, so it has to sit between the sender starting
# and the wait for completion.
if [ "$PROBE_DATA_PLANE" == "true" ]; then
    source "$SCRIPT_DIR/lib/probe.sh"
    probe_data_plane || exit 1
fi

# Memory sampling: on when the scenario asks to be measured, and implied by declaring a ceiling
if [ "$SAMPLE_RSS" == "true" ] || [ -n "$MAX_RECEIVER_RSS_MB" ] || [ -n "$MAX_SENDER_RSS_MB" ]; then
    for rpid in "${RECEIVER_PIDS[@]}"; do sample_rss_for receiver "$rpid"; done
    if [ -n "$SENDER_PID" ]; then sample_rss_for sender "$SENDER_PID"; fi
    start_rss_sampler
fi

# Wait for receivers to complete. A scenario that expects a fault to be survived declares a
# deadline: without one, a receiver whose sender gave up is only released by the hub's bridge
# timeout, minutes later, and the run reads as hung rather than failed.
if ! wait_for_receivers; then
    log_error "--- What the sender said ---"
    if [ -n "$SENDER_PID" ] && ps -p "$SENDER_PID" > /dev/null 2>&1; then
        log_error "Sender is still running."
    fi
    grep -h "TRANSFER FAILED\|failed\|Failed" "$OUTPUT_DIR"/sender*.log 2>/dev/null | sort -u | head -n 5
    log_error "-----------------------------"
    if [ "$ASSERT_TRANSFER_AFFINITY" == "true" ]; then
        log_error "--- Where the streams went ---"
        check_stream_routing || true
        log_error "------------------------------"
    fi
    exit 1
fi
if ! wait_for_sender; then
    log_error "❌ Error: the sender was still running ${SENDER_EXIT_WAIT_SECONDS}s after the receivers finished"
    exit 1
fi
stop_rss_sampler

# Validation
echo -e "${GREEN}==================================================${NC}"
log_info "Benchmark completed."

EXPECTED_FILES=$NUM_SENDERS
FOUND_FILES=$(find "$OUTPUT_DIR" -name "orders_*.csv" 2>/dev/null | wc -l | tr -d ' ')

if [ "$FOUND_FILES" -ne "$EXPECTED_FILES" ]; then
    log_error "❌ Error: Expected $EXPECTED_FILES CSV files, found $FOUND_FILES"
    exit 1
fi

TOTAL_LINES=0
EXPECTED_LINES=${NUM_ITEMS:-2000000}

for f in "$OUTPUT_DIR"/orders_*.csv; do
    [ -e "$f" ] || continue
    FILE_LINES=$(wc -l < "$f" | tr -d ' ')
    echo "File $(basename "$f"): $FILE_LINES lines"
    
    if [ "$FILE_LINES" -ne "$EXPECTED_LINES" ]; then
        log_error "❌ Error: $(basename "$f") has $FILE_LINES lines (expected $EXPECTED_LINES)"
        log_error "--- Receiver Log Tail ---"
        tail -n 20 "$OUTPUT_DIR/receiver.log" 2>/dev/null
        log_error "-------------------------"
        exit 1
    fi
    TOTAL_LINES=$((TOTAL_LINES + FILE_LINES))
done

# A single fault is only a test if it fell while the transfer was under way. Both moments are
# recorded with the sender's liveness by start_hold_injection; a hold that fired after the sender
# had finished proves nothing, whatever the verdict.
if [ "$ENABLE_CHAOS" == "true" ] && [ "${CHAOS_PROGRAM:-loop}" == "hold" ]; then
    events="$OUTPUT_DIR/chaos-events.log"
    if ! grep -q "^held sender_alive=yes" "$events" 2>/dev/null \
       || ! grep -q "^released sender_alive=yes" "$events" 2>/dev/null; then
        log_error "❌ Error: the fault did not fall mid-transfer. chaos-events.log:"
        cat "$events" >&2 2>/dev/null
        exit 1
    fi
    log_info "Fault landed mid-transfer: $(tr '\n' ';' < "$events")"
fi

# Every stream request of a transfer belongs on the hub that holds its bridge, and neither the line
# counts nor the senders' verdict looks at where a request went.
if [ "$ASSERT_TRANSFER_AFFINITY" == "true" ]; then
    check_stream_routing || { log_error "❌ Error: the streams of a transfer did not all reach the hub that holds it"; exit 1; }
    log_info "Streams of each transfer reached a single hub."
fi

assert_peak_rss receiver "$MAX_RECEIVER_RSS_MB" || exit 1
assert_peak_rss sender "$MAX_SENDER_RSS_MB" || exit 1

# A finished transfer whose handle was released leaves the client's registry. A client still
# holding one has leaked it, and no line count or memory ceiling would say so.
for role in SENDER RECEIVER; do
    live_line=$(wait_for_log_lines "\[$role\]\[SESSIONS\]" | tail -1)
    if [ -z "$live_line" ]; then
        log_error "❌ Error: $role never reported its live session count"
        exit 1
    fi
    live_count=$(echo "$live_line" | sed -E 's/.*\[SESSIONS\] ([0-9]+) live.*/\1/')
    if [ "$live_count" != "0" ]; then
        log_error "❌ Error: $role still holds $live_count transfer(s) after completion"
        exit 1
    fi
done
log_info "Sessions released: sender and receiver both hold 0 transfers."

# The receiver writes every row before the sender learns whether its Complete was acknowledged, so
# a transfer that failed still leaves a full CSV and an empty session registry behind. Neither the
# line count nor the two checks above can tell that apart from a success - only the sender's own
# verdict can, and it is logged once per sender on the path where CompleteAsync returned.
# Counted by distinct transfer id because OUTPUT_DIR holds both the console capture and the
# structured log, and every line appears in both.
completed_key='s/.*\[SENDER\]\[STATS\] Transfer ([0-9a-zA-Z]+) complete:.*/\1/'
completed_senders=$(wait_for_log_lines "\[SENDER\]\[STATS\] Transfer .* complete:" "$NUM_SENDERS" "$completed_key" \
    | sed -E "$completed_key" | sort -u | wc -l | tr -d ' ')

if [ "$completed_senders" -ne "$NUM_SENDERS" ]; then
    log_error "❌ Error: $completed_senders of $NUM_SENDERS sender(s) reported a completed transfer"
    grep -h "TRANSFER FAILED" "$OUTPUT_DIR"/*.log 2>/dev/null | sort -u | head -n 5
    exit 1
fi
log_info "Senders acknowledged: $completed_senders of $NUM_SENDERS reported a completed transfer."

log_info "✅ $SCENARIO_NAME PASSED! Total: $TOTAL_LINES lines."
