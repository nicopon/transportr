#!/bin/bash
# scripts/lib/byte-stream.sh - Judging a run of the byte-stream mode
#
# The sender copies data/orders.bin as it is and the receiver writes it as it is, so nothing in the
# output is a line or an item: the run is judged on the size and the SHA-256 of what was written, which
# a truncated or a corrupted file cannot keep.

file_size() {
    wc -c < "$1" | tr -d ' '
}

file_sha256() {
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

# Every receiver wrote one file, and each is the source file. Sets TOTAL_BYTES to what they hold.
check_byte_stream_output() {
    local source="$PROJECT_ROOT/data/orders.bin"
    local expected_files=$NUM_SENDERS
    local found_files
    found_files=$(find "$OUTPUT_DIR" -name "orders_*.bin" 2>/dev/null | wc -l | tr -d ' ')

    if [ "$found_files" -ne "$expected_files" ]; then
        log_error "❌ Error: Expected $expected_files .bin files, found $found_files"
        return 1
    fi

    local expected_size expected_hash
    expected_size=$(file_size "$source")
    expected_hash=$(file_sha256 "$source")

    TOTAL_BYTES=0
    local f size hash
    for f in "$OUTPUT_DIR"/orders_*.bin; do
        [ -e "$f" ] || continue
        size=$(file_size "$f")
        echo "File $(basename "$f"): $size bytes"

        if [ "$size" -ne "$expected_size" ]; then
            log_error "❌ Error: $(basename "$f") has $size bytes (expected $expected_size)"
            log_error "--- Receiver Log Tail ---"
            tail -n 20 "$OUTPUT_DIR/receiver.log" 2>/dev/null
            log_error "-------------------------"
            return 1
        fi

        hash=$(file_sha256 "$f")
        if [ "$hash" != "$expected_hash" ]; then
            log_error "❌ Error: $(basename "$f") has SHA-256 $hash (expected $expected_hash)"
            log_error "--- Receiver Log Tail ---"
            tail -n 20 "$OUTPUT_DIR/receiver.log" 2>/dev/null
            log_error "-------------------------"
            return 1
        fi
        TOTAL_BYTES=$((TOTAL_BYTES + size))
    done
}
