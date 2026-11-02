#!/bin/bash
# scripts/lib/affinity.sh - Where things landed behind the TestProxy, for a scenario about affinity
#
# Two hubs sit behind the TestProxy, and a client with no affinity cookie goes where the proxy's
# balancing policy sends it. A scenario about affinity needs the placement as a fact rather than a
# hope: where each receiver registered is read from the hubs' own logs, and which hub each stream
# request of a transfer reached is read from the proxy's.

# The hub (a or b) whose log says the client registered on it; non-zero when neither does.
hub_holding_client() {
    local client_id=$1 hub
    for hub in a b; do
        if grep -qsF "\"$client_id\" connected" "$OUTPUT_DIR"/hub_$hub/hub*.log; then
            echo "$hub"
            return 0
        fi
    done
    return 1
}

# Run once the receivers are up and before the sender starts. A run whose receivers share a hub
# proves nothing about affinity, so it stops here with a message of its own, not as a failed transfer.
assert_receivers_on_distinct_hubs() {
    local placement="$OUTPUT_DIR/placement.log"
    local i attempt client_id hub placed=()
    : > "$placement"

    for ((i=1; i<=NUM_RECEIVERS; i++)); do
        client_id=""
        hub=""
        for attempt in $(seq 1 20); do
            client_id=$(sed -nE 's/.*Client registered \(ID=([0-9a-f-]+),.*/\1/p' "$OUTPUT_DIR/receiver_$i.log" 2>/dev/null | head -1)
            [ -n "$client_id" ] && hub=$(hub_holding_client "$client_id") && break
            sleep 0.5
        done

        if [ -z "$hub" ]; then
            log_error "❌ Setup error: receiver $i did not register on a hub within 10s"
            return 1
        fi
        echo "receiver $i ($client_id) registered on hub $hub" | tee -a "$placement"
        placed+=("$hub")
    done

    if [ -n "$(printf '%s\n' "${placed[@]}" | sort | uniq -d)" ]; then
        log_error "❌ Setup error: the receivers did not land on distinct hubs (${placed[*]}); see $placement"
        return 1
    fi
    log_info "Receivers placed on distinct hubs: ${placed[*]}"
}

# Reads the proxy's log for the stream requests of every transfer and says which hub each one reached.
# Exit status: 0 when every transfer's requests reached a single hub; 1 when one transfer's
# requests were split across hubs, which is the fault this check exists to find; 2 when the run did
# not set the scene (no stream request, the first transfer's sender stream was never reopened, or it
# was reopened before the second transfer opened), in which case a verdict on routing means nothing.
# The summary is also written to stream-routing.log.
check_stream_routing() {
    local report="$OUTPUT_DIR/stream-routing.log"
    local output code=0

    output=$(sed -nE 's#.*Proxying to http://localhost:([0-9]+)/api/stream/([0-9a-f-]+)/(send|receive)( |\?).*#\2 \3 \1#p' "$OUTPUT_DIR/proxy.log" 2>/dev/null \
        | awk '
            {
                id = $1; verb = $2; port = $3
                if (!(id in rank)) { rank[id] = ++transfers; ids[transfers] = id; firstSeen[id] = NR }
                timeline = timeline (timeline == "" ? "" : ", ") "#" rank[id] " " verb "@" port
                if (!((id, port) in reached)) {
                    reached[id, port] = 1
                    hubs[id] = hubs[id] (hubs[id] == "" ? "" : " ") port
                    hubCount[id]++
                }
                if (verb == "send") { sends[id]++; lastSend[id] = NR }
            }
            END {
                if (transfers == 0) { print "SETUP: the proxy forwarded no stream request"; exit 2 }
                print "stream requests, in the order the proxy forwarded them:"
                print "  " timeline
                splitFound = 0
                for (t = 1; t <= transfers; t++) {
                    id = ids[t]
                    print "transfer #" t " " id " reached: " hubs[id]
                    if (hubCount[id] > 1) {
                        print "SPLIT: transfer #" t " " id " had its streams on hubs " hubs[id]
                        splitFound = 1
                    }
                }
                first = ids[1]
                if (sends[first] < 2) { print "SETUP: the first transfer'"'"'s sender stream was never reopened"; exit 2 }
                if (transfers > 1 && lastSend[first] < firstSeen[ids[2]]) {
                    print "SETUP: the first transfer'"'"'s sender stream was reopened before the second transfer opened"; exit 2
                }
                exit splitFound
            }') || code=$?

    echo "$output" | tee "$report"
    return $code
}
