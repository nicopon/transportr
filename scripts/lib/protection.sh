#!/bin/bash
# scripts/lib/protection.sh - What protected a run's transfers, and whether the key stayed secret
#
# Each sample journals, per transfer, "[SENDER][PROTECTION] Transfer <id> protection: <level>" (the
# receiver the same with [RECEIVER]), the level being what TransferInfo.Protection says at the end of
# the transfer. Read from the structured log of each side, like lib/transport.sh does.
#
# Declared by the scenario:
#   ASSERT_PROTECTION   None, Encrypted or EncryptedWithPreSharedKey: every transfer of both sides says so
#   E2E_PRE_SHARED_KEY  true: run-benchmark.sh generated a key for this run (E2E_KEY); it must be in no file of OUTPUT_DIR

# Both sides said the level, for as many transfers as there are senders, and no line says another.
check_protection() {
    local side tag lines stray count
    for side in sender receiver; do
        tag=$(echo "$side" | tr '[:lower:]' '[:upper:]')

        if ! lines=$(side_lines "$side" "$NUM_SENDERS" "\[$tag\]\[PROTECTION\] Transfer"); then
            log_error "❌ Error: the $side did not say the protection of $NUM_SENDERS transfer(s)"
            echo "$lines" >&2
            return 1
        fi
        stray=$(echo "$lines" | grep -v "protection: $ASSERT_PROTECTION\$" || true)
        if [ -n "$stray" ]; then
            log_error "❌ Error: the $side reported a protection other than $ASSERT_PROTECTION:"
            echo "$stray" >&2
            return 1
        fi
        count=$(echo "$lines" | sed -E 's/.*Transfer ([0-9a-zA-Z]+) protection:.*/\1/' | sort -u | wc -l | tr -d ' ')
        log_info "Protection of the $side: $count transfer(s), all $ASSERT_PROTECTION."
    done
}

# The key was only ever in the environment of the two samples: no log, capture or output file of
# the run (the hub's included) may hold it.
check_key_absent() {
    local holders
    holders=$(grep -rlaF -e "$E2E_KEY" "$OUTPUT_DIR" 2>/dev/null || true)
    if [ -n "$holders" ]; then
        log_error "❌ Error: the pre-shared key of this run appears in:"
        echo "$holders" >&2
        return 1
    fi
    log_info "The pre-shared key of this run is in no file of $OUTPUT_DIR ($(find "$OUTPUT_DIR" -type f | wc -l | tr -d ' ') files searched)."
}
