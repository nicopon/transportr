#!/bin/bash
# scripts/lib/transport.sh - What carried the write streams of a run, from what the clients journal
#
# A client logs "Write stream (<Type>) of Transfer=<id> opened over <Transport>" at each opening of a
# write stream, and "No WebSocket for the write stream of Transfer=<id>: <reason>" when it gives up on
# a WebSocket. What a sender writes is its Data stream and what a receiver writes is its Control
# stream, so a type also says which side a line is from.
#
# Every line is counted in one family of files per side, the structured log (the file sink adds the
# date to the name): OUTPUT_DIR also holds the receiver's console capture, where each of its lines
# appears a second time.
#
# Declared by the scenario:
#   ASSERT_WRITE_TRANSPORT      WebSocket or Segments: every opening of both sides says so
#   EXPECT_WRITE_FALLBACK       true: each side gave up on a WebSocket and said why. Left unset with
#                               ASSERT_WRITE_TRANSPORT=WebSocket, no side did
#   EXPECT_WEBSOCKET_REOPENED   true: a transfer's Data stream was opened over a WebSocket at least twice
#   EXPECT_RESUME               true: each side behind the proxy (CHAOS_CLIENTS) went from Streaming to
#                               Reconnecting and back to Streaming, so the transfer was cut and resumed
#   EXPECT_TRANSFER_FAILURE     true: the run is meant to fail, and expect_transfer_failure judges how
#   EXPECT_FAILURE_MESSAGE      what the sender's failure message must contain (default: requires a WebSocket)
#   EXPECT_HUB_LOG              what the hub must have logged (default: not opened over a WebSocket); set
#                               empty when the hub is not meant to have seen the request

SENDER_LOGS="sender[0-9]*.log"
RECEIVER_LOGS="receiver[0-9]*.log"

# Prints the lines of one side's structured log matching the grep pattern $3, waiting for late ones as
# wait_for_log_lines does. $1 is the side (sender or receiver), $2 how many such lines are wanted.
# The file sink renders an enum between quotes, which the console does not: a pattern allows for
# them, and the lines come back without.
side_lines() {
    local side=$1 wanted=$2 pattern=$3 glob lines status=0
    case "$side" in
        sender)   glob="$SENDER_LOGS" ;;
        receiver) glob="$RECEIVER_LOGS" ;;
    esac
    lines=$(wait_for_log_lines "$pattern" "$wanted" "" "$glob") || status=$?
    echo "$lines" | tr -d '"'
    return $status
}

# The reason on a "No WebSocket for" line, whatever the line's timestamp.
fallback_reason() {
    sed -E 's/.*No WebSocket for the write stream of Transfer=[^ ]+: //'
}

check_write_transport() {
    local side type lines stray

    if [ -n "$ASSERT_WRITE_TRANSPORT" ]; then
        local other="Segments"
        [ "$ASSERT_WRITE_TRANSPORT" == "Segments" ] && other="WebSocket"

        for side in sender:Data receiver:Control; do
            type=${side#*:}
            side=${side%%:*}

            if ! lines=$(side_lines "$side" 1 "Write stream (.\{0,1\}$type.\{0,1\}) of Transfer=.* opened over"); then
                log_error "❌ Error: the $side never said what its $type stream was opened over"
                return 1
            fi
            stray=$(echo "$lines" | grep -v "opened over $ASSERT_WRITE_TRANSPORT\$" || true)
            if [ -n "$stray" ]; then
                log_error "❌ Error: the $side opened a stream over $other, not only over $ASSERT_WRITE_TRANSPORT:"
                echo "$stray" >&2
                return 1
            fi
            log_info "Write stream of the $side ($type): $(echo "$lines" | wc -l | tr -d ' ') opening(s), all over $ASSERT_WRITE_TRANSPORT."
        done
    fi

    if [ "$EXPECT_WRITE_FALLBACK" == "true" ]; then
        for side in sender receiver; do
            if ! lines=$(side_lines "$side" 1 "No WebSocket for the write stream of Transfer=.*: ."); then
                log_error "❌ Error: the $side never said why it has no WebSocket for its write stream"
                return 1
            fi
            echo "$lines" | fallback_reason | sort -u | while IFS= read -r reason; do
                log_info "Fallback of the $side: $reason"
            done
        done
    elif [ "$ASSERT_WRITE_TRANSPORT" == "WebSocket" ]; then
        stray=$(cat "$OUTPUT_DIR"/$SENDER_LOGS "$OUTPUT_DIR"/$RECEIVER_LOGS 2>/dev/null \
            | grep "No WebSocket for the write stream" || true)
        if [ -n "$stray" ]; then
            log_error "❌ Error: a client gave up on its WebSocket in a run that expects none:"
            echo "$stray" >&2
            return 1
        fi
    fi

    if [ "$EXPECT_WEBSOCKET_REOPENED" == "true" ]; then
        local transfer
        lines=$(side_lines sender 2 "Write stream (.\{0,1\}Data.\{0,1\}) of Transfer=.* opened over .\{0,1\}WebSocket") || true
        transfer=$(echo "$lines" | sed -nE 's/.*Transfer=([^ ]+) opened over WebSocket/\1/p' | sort | uniq -c | awk '$1 >= 2 { print $2; exit }')
        if [ -z "$transfer" ]; then
            log_error "❌ Error: no transfer had its Data stream opened over a WebSocket twice"
            echo "$lines" >&2
            return 1
        fi
        log_info "Data stream of transfer $transfer opened over a WebSocket $(echo "$lines" | grep -c "Transfer=$transfer opened over WebSocket") times."
    fi

    return 0
}

# What a sender's failure may tell its host: the message the client raises, without a path, an
# address, a group or a setting.
GENERIC_MESSAGE_FORBIDS='/api/|://|localhost|senders|receivers|transportr:group|EnableAuth|Sender__|Receiver__'

# For a run that must end in a refusal the client reports as a failure of the transfer. Waits for the
# sender to exit, within TRANSFER_TIMEOUT_SECONDS, then requires that it said the transfer failed, with
# the generic message the scenario names, that the hub logged what the scenario expects of it, and
# that no CSV holds every line. It then watches the receivers until the same deadline, to say what
# became of them, and does not judge it.
expect_transfer_failure() {
    local started=$(date +%s)
    local deadline=$(( started + ${TRANSFER_TIMEOUT_SECONDS:?an expected failure needs TRANSFER_TIMEOUT_SECONDS} ))

    while ps -p "$SENDER_PID" > /dev/null 2>&1; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            log_error "❌ Error: the sender was still running ${TRANSFER_TIMEOUT_SECONDS}s after it started"
            return 1
        fi
        sleep 1
    done
    wait "$SENDER_PID" 2>/dev/null || true
    log_info "Sender ended $(( $(date +%s) - started ))s after it started."

    local failed message
    if ! failed=$(side_lines sender 1 "Transfer .* failed"); then
        log_error "❌ Error: the sender never said that its transfer failed"
        return 1
    fi
    message=$(cat "$OUTPUT_DIR"/$SENDER_LOGS 2>/dev/null | sed -nE 's/.*TransferFailedException: (.*)/\1/p' | head -1)
    local expected_message="${EXPECT_FAILURE_MESSAGE:-requires a WebSocket}"
    if ! echo "$message" | grep -q "$expected_message"; then
        log_error "❌ Error: the sender's failure is not the expected one ($expected_message): ${message:-<no message>}"
        return 1
    fi
    if echo "$message" | grep -qE "$GENERIC_MESSAGE_FORBIDS"; then
        log_error "❌ Error: the sender's failure message is not generic: $message"
        return 1
    fi
    log_info "Sender: $(echo "$failed" | head -1 | sed -E 's/^[^ ]+ [^ ]+ [^ ]+ //') | $message"

    # Each hub of an HA run logs in a directory of its own.
    local hub_logs="hub[0-9]*.log"
    [ "$ENABLE_HA" == "true" ] && hub_logs="hub_?/hub[0-9]*.log"
    local hub_expects="${EXPECT_HUB_LOG-not opened over a WebSocket}"
    if [ -n "$hub_expects" ]; then
        if ! wait_for_log_lines "$hub_expects" 1 "" "$hub_logs" > /dev/null; then
            log_error "❌ Error: the hub never logged: $hub_expects"
            return 1
        fi
        log_info "Hub: logged \"$hub_expects\"."
    fi

    local f lines
    for f in "$OUTPUT_DIR"/orders_*.csv; do
        [ -e "$f" ] || continue
        lines=$(wc -l < "$f" | tr -d ' ')
        if [ "$lines" -eq "${NUM_ITEMS:-2000000}" ]; then
            log_error "❌ Error: $(basename "$f") holds every line of a transfer that was meant to fail"
            return 1
        fi
        log_info "$(basename "$f"): $lines of ${NUM_ITEMS:-2000000} lines."
    done

    local pid pending
    while true; do
        pending=0
        for pid in "${RECEIVER_PIDS[@]}"; do
            if ps -p "$pid" > /dev/null 2>&1; then pending=$((pending + 1)); fi
        done
        if [ "$pending" -eq 0 ]; then
            log_info "Receivers: all ended $(( $(date +%s) - started ))s after the sender started."
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            log_info "Receivers: $pending still running at the ${TRANSFER_TIMEOUT_SECONDS}s bound."
            return 0
        fi
        sleep 1
    done
}

# A hold that falls mid-transfer says the fault landed, not that the transfer lived through it: a transfer
# that survived without its streams ever failing would pass too. The session journals each state change.
check_resume() {
    local side lines
    for side in sender receiver; do
        case "$CHAOS_CLIENTS" in
            both|"$side") ;;
            *) continue ;;
        esac

        if ! side_lines "$side" 1 "State Transition: .\{0,1\}Streaming.\{0,1\} -> .\{0,1\}Reconnecting" > /dev/null; then
            log_error "❌ Error: the $side was cut and its transfer never went from Streaming to Reconnecting"
            return 1
        fi
        if ! lines=$(side_lines "$side" 1 "State Transition: .\{0,1\}Reconnecting.\{0,1\} -> .\{0,1\}Streaming"); then
            log_error "❌ Error: the $side's transfer went to Reconnecting and never back to Streaming"
            return 1
        fi
        log_info "The $side's transfer resumed: $(echo "$lines" | wc -l | tr -d ' ') return(s) to Streaming."
    done
}
