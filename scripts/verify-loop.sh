#!/bin/bash
set -e

count=4
echo "Starting robustness verification ($count loops of test-all.sh)..."

for i in $(seq 1 $count); do
    echo "=================================================="
    echo "LOOP $i / $count"
    echo "=================================================="
    
    if ./scripts/test-all.sh; then
        echo "✅ LOOP $i PASSED"
    else
        echo "❌ LOOP $i FAILED"
        exit 1
    fi
    
    # Wait a bit between runs to let ports release
    sleep 5
done

echo "🎉 ALL $count LOOPS PASSED SUCCESSFULLY!"
