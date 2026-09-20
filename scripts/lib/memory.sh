#!/bin/bash
# scripts/lib/memory.sh - RSS sampling for benchmark scenarios
#
# A scenario about memory needs memory measured, and nothing here measured any until now. The
# sampler writes one row per process per second to $OUTPUT_DIR/rss.csv; a scenario then declares a
# ceiling (MAX_RECEIVER_RSS_MB / MAX_SENDER_RSS_MB) and run-benchmark.sh fails the run when the
# peak goes over it, the same way it fails on a wrong line count.

declare -a SAMPLED_PROCESSES=()
RSS_SAMPLER_PID=""

# Register a process to sample: sample_rss_for <role> <pid>
sample_rss_for() {
    SAMPLED_PROCESSES+=("$1:$2")
}

rss_csv_path() {
    echo "${OUTPUT_DIR:-output}/rss.csv"
}

start_rss_sampler() {
    if [ ${#SAMPLED_PROCESSES[@]} -eq 0 ]; then return 0; fi

    local out
    out="$(rss_csv_path)"
    echo "elapsed_s,role,pid,rss_kb" > "$out"

    local specs="${SAMPLED_PROCESSES[*]}"
    log_step "Sampling RSS for: $specs"

    (
        local start
        start=$(date +%s)
        while true; do
            local now=$(( $(date +%s) - start ))
            for spec in $specs; do
                local role="${spec%%:*}"
                local pid="${spec##*:}"
                local rss
                rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
                [ -n "$rss" ] && echo "$now,$role,$pid,$rss" >> "$out"
            done
            sleep 1
        done
    ) &

    RSS_SAMPLER_PID=$!
    register_pid $RSS_SAMPLER_PID
    # Off the job table: stopping the sampler is routine, and a "Terminated" notice in the middle
    # of the validation output reads like a failure.
    disown $RSS_SAMPLER_PID 2>/dev/null || true
}

stop_rss_sampler() {
    if [ -n "$RSS_SAMPLER_PID" ]; then kill $RSS_SAMPLER_PID 2>/dev/null; fi
    RSS_SAMPLER_PID=""
    return 0
}

# Peak RSS in MB for a role, or 0 when nothing was sampled for it.
peak_rss_mb() {
    local out
    out="$(rss_csv_path)"
    [ -f "$out" ] || { echo 0; return 0; }
    awk -F, -v role="$1" 'NR > 1 && $2 == role && $4 + 0 > max { max = $4 + 0 } END { printf "%d", max / 1024 }' "$out"
}

# Fail the run when a role's peak went over its declared ceiling.
assert_peak_rss() {
    local role="$1"
    local ceiling_mb="$2"
    if [ -z "$ceiling_mb" ]; then return 0; fi

    local peak
    peak=$(peak_rss_mb "$role")
    echo "Peak $role RSS: ${peak} MB (ceiling ${ceiling_mb} MB)"

    if [ "$peak" -gt "$ceiling_mb" ]; then
        log_error "❌ Error: $role peaked at ${peak} MB, over the ${ceiling_mb} MB ceiling"
        return 1
    fi
    return 0
}
