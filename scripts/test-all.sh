#!/usr/bin/env bash
# scripts/test-all.sh - All-in-one integration test runner
#
# This script runs all benchmark scenarios sequentially to validate non-regression.
#
# Usage: ./scripts/test-all.sh
#
# Steps:
#   1. Initialize test data (2,000,000 orders) and download tools
#   2. Build solution in Release mode
#   3. Run all scenarios: default, embedded-idp, oidc, chaos, chaos-bounded, slow-receiver,
#      ha-redis
#

set -e

# Determine script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Scenarios to run (in order)
SCENARIOS="default embedded-idp oidc chaos chaos-bounded slow-receiver ha-redis"

# Track results
FAILED=0
PASSED=0
RESULTS=""
START_TIME=$(date +%s)

# Banner
echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  TransportR - Integration Test Suite${NC}"
echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Scenarios to run: ${SCENARIOS}"
echo ""

# Step 1: Initialization
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Step 1/3: Initialization${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if ! "$SCRIPT_DIR/init.sh"; then
    echo -e "${RED}❌ Initialization failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Initialization complete${NC}"
echo ""

# Step 2: Build
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Step 2/3: Build${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if ! "$SCRIPT_DIR/build_release.sh"; then
    echo -e "${RED}❌ Build failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Build complete${NC}"
echo ""

# Step 3: Run all scenarios
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Step 3/3: Running Scenarios${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

for scenario in $SCENARIOS; do
    echo -e "${BLUE}▶ Running scenario: ${scenario}${NC}"
    
    SCENARIO_START=$(date +%s)
    
    # Disable set -e temporarily to capture failure
    set +e
    "$SCRIPT_DIR/run-benchmark.sh" --no-build "$scenario"
    EXIT_CODE=$?
    set -e
    
    SCENARIO_END=$(date +%s)
    DURATION=$((SCENARIO_END - SCENARIO_START))
    
    if [ $EXIT_CODE -eq 0 ]; then
        RESULTS="${RESULTS}  - ${scenario}: ✅ PASSED (${DURATION}s)\n"
        PASSED=$((PASSED + 1))
        echo -e "${GREEN}✅ Scenario '${scenario}' PASSED (${DURATION}s)${NC}"
    else
        RESULTS="${RESULTS}  - ${scenario}: ❌ FAILED (${DURATION}s)\n"
        FAILED=$((FAILED + 1))
        echo -e "${RED}❌ Scenario '${scenario}' FAILED (${DURATION}s)${NC}"
    fi
    
    echo ""
done

# Summary
END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Results:"
echo -e "$RESULTS"
echo "Total: ${PASSED} passed, ${FAILED} failed"
echo "Duration: ${TOTAL_DURATION}s"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  ✅ ALL TESTS PASSED${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
    exit 0
else
    echo -e "${RED}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}  ❌ SOME TESTS FAILED${NC}"
    echo -e "${RED}══════════════════════════════════════════════════════════════${NC}"
    exit 1
fi
