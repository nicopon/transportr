#!/bin/bash
set -e

# scripts/benchmark-compare.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Ensure we start with a fresh build
echo "Build solution..."
./scripts/build_release.sh > /dev/null

RESULTS_FILE="benchmark_results.txt"
rm -f "$RESULTS_FILE"
echo "Serializer,Iteration,Throughput" > "$RESULTS_FILE"

run_benchmark() {
    local serializer=$1
    local iteration=$2
    
    echo "--------------------------------------------------"
    echo "Running $serializer (Iteration $iteration/3)..."
    
    # Run benchmark with specific serializer
    # We suppress standard output but capture the result line
    export SERIALIZER_TYPE="$serializer"
    
    # Run, capture output to temp file to avoid noise but allow grep
    local log_file="benchmark_${serializer}_${iteration}.log"
    
    if ./scripts/run-benchmark.sh --no-build default > "$log_file" 2>&1; then
        # Parse throughput
        # Look for: [SENDER][STATS] Transfer ... complete: ... items | ... ACKs received | 123,456 items/s
        local throughput_line=$(grep "\[SENDER\]\[STATS\] Transfer .* complete" "$log_file")
        
        if [ -n "$throughput_line" ]; then
            # Extract number before " items/s"
            # Remove command grouping separators if any
            local throughput=$(echo "$throughput_line" | awk -F'|' '{print $3}' | awk '{print $1}' | tr -d ',')
            echo "$serializer,$iteration,$throughput" >> "$RESULTS_FILE"
            echo "Result: $throughput items/s"
        else
            echo "Error: Could not parse throughput stats."
            echo "$serializer,$iteration,0" >> "$RESULTS_FILE"
        fi
    else
        echo "Error: Benchmark run failed."
        echo "$serializer,$iteration,0" >> "$RESULTS_FILE"
        cat "$log_file" | tail -n 20
    fi
    
    # Cleanup logs to save space (optional)
    rm "$log_file"
}

# 1. MessagePack (Baseline)
for i in {1..3}; do
    run_benchmark "MessagePack" $i
done

# 3. Json (Zstd)
for i in {1..3}; do
    run_benchmark "Json" $i
done



echo ""
echo "=================================================="
echo "BENCHMARK RESULTS SUMMARY"
echo "=================================================="

# Calculate averages using awk
awk -F, '
NR>1 {
    sum[$1] += $3
    count[$1]++
    vals[$1] = vals[$1] " " $3
}
END {
    printf "%-15s | %-15s | %s\n", "Serializer", "Avg Items/s", "Raw Runs"
    print "----------------|-----------------|--------------------------------"
    for (s in sum) {
        if (count[s] > 0) {
            avg = sum[s] / count[s]
            printf "%-15s | %-15.0f |%s\n", s, avg, vals[s]
        }
    }
}
' "$RESULTS_FILE"
