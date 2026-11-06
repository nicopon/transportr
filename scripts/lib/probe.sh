#!/bin/bash
# scripts/lib/probe.sh - Data plane access probe.
#
# Asks the running hub, mid-transfer, the one thing the endpoint tests cannot ask of the sample
# host: that its own composition of MapControllers/MapSimpleIdp/MapDataHub still refuses a caller
# who is not a participant, while the legitimate transfer runs to completion.
#
# Every probe uses an identity that must be refused, so none of them can evict a live stream. That
# holds for a request that asks to upgrade as for any other: a refused one never opens a socket, and a
# socket takes its place on a bridge only when it carries its first byte, which a probe never sends.

probe_status() {
    local method="$1" route="$2" conn_type="$3" token="$4"
    local args=(-s -o /dev/null -w '%{http_code}' -X "$method" -H "X-Connection-Type: $conn_type")

    if [ -n "$token" ]; then
        args+=(-H "Authorization: Bearer $token")
    fi

    curl "${args[@]}" "$HUB_URL/api/stream/$PROBE_TRANSFER_ID/$route"
}

expect_status() {
    local expected="$1" label="$2" method="$3" route="$4" conn_type="$5" token="$6"
    local actual
    actual=$(probe_status "$method" "$route" "$conn_type" "$token")

    if [ "$actual" != "$expected" ]; then
        log_error "❌ Probe: $label returned $actual (expected $expected)"
        return 1
    fi

    log_info "  Probe: $label -> $actual"
    return 0
}

# A write stream opened as a WebSocket client does, by a GET that asks to upgrade. Bounded, because a
# hub that did accept it would hold the request open.
probe_upgrade_status() {
    local conn_type="$1" token="$2"
    local args=(-s -m 5 --http1.1 -o /dev/null -w '%{http_code}'
        -H "Connection: Upgrade" -H "Upgrade: websocket"
        -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ=="
        -H "X-Connection-Type: $conn_type")

    if [ -n "$token" ]; then
        args+=(-H "Authorization: Bearer $token")
    fi

    curl "${args[@]}" "$HUB_URL/api/stream/$PROBE_TRANSFER_ID/send?heartbeatMs=15000"
}

expect_upgrade_status() {
    local expected="$1" label="$2" conn_type="$3" token="$4"
    local actual
    actual=$(probe_upgrade_status "$conn_type" "$token")

    if [ "$actual" != "$expected" ]; then
        log_error "❌ Probe: $label returned $actual (expected $expected)"
        return 1
    fi

    log_info "  Probe: $label -> $actual"
    return 0
}

wait_for_live_transfer() {
    local deadline=$((SECONDS + 60))

    while [ $SECONDS -lt $deadline ]; do
        PROBE_TRANSFER_ID=$(grep -ohE 'BRIDGE: Piping [0-9a-f-]{36}:Data' "$OUTPUT_DIR"/hub*.log 2>/dev/null \
            | head -1 | grep -oE '[0-9a-f-]{36}')

        if [ -n "$PROBE_TRANSFER_ID" ]; then
            return 0
        fi
        sleep 1
    done

    log_error "❌ Probe: no transfer reached the piping state within 60s"
    return 1
}

probe_data_plane() {
    wait_for_live_transfer || return 1
    log_step "Probing data plane on live transfer $PROBE_TRANSFER_ID..."

    expect_status 401 "anonymous read"           GET  receive Data "" || return 1
    expect_status 401 "anonymous write"          POST send    Data "" || return 1
    expect_status 404 "sender reading data"      GET  receive Data "$SENDER_TOKEN"   || return 1
    expect_status 404 "receiver writing data"    POST send    Data "$RECEIVER_TOKEN" || return 1

    expect_upgrade_status 401 "anonymous write, upgrade"       Data ""                || return 1
    expect_upgrade_status 404 "receiver writing data, upgrade" Data "$RECEIVER_TOKEN" || return 1

    return 0
}
