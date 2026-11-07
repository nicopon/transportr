#!/bin/bash
# scripts/client-bench.sh - Compare builds of the client by the CPU time they spend per MiB carried
#
# One hub, built once and run apart from every arm; each arm is a Release build of
# tests/TransportR.ClientBench in a directory of its own, run directly. The arms are interleaved round
# after round in this one sitting, and the first arm runs a second time under another label (the A/A
# witness), because two sittings on this machine differ by more than the changes worth measuring.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

usage() {
    cat <<'USAGE'
Usage: scripts/client-bench.sh [options] ARM [ARM...]

ARM is the directory of a Release build of the harness, or LABEL=DIRECTORY. Build one per version:

    dotnet build tests/TransportR.ClientBench -c Release -o /tmp/arm-before

The arms run in turn, round after round (the order reverses every other round), and the first arm
runs once more per round under the label <first>-aa: the same binary, which shows how far two runs of
one build differ in this sitting. A difference between two arms is worth reading only beyond that gap.

Options:
  --mode items|bytes       MessagePack items, or the byte-stream mode (default: bytes)
  --rounds N               runs of each arm (default: 20)
  --megabytes N            MiB of batch body of each measured transfer (harness default)
  --warmup-megabytes N     MiB of the discarded first transfer of each run (harness default)
  --seed N                 seed of the pseudo-random bytes (harness default)
  --no-aa                  no A/A witness
  --hub-port N             port of the hub (default: 5352); the script refuses a busy port
  --no-hub-build           use the hub already built under samples/DataHub
  --help                   this text

Prints, per arm, the median, mean, minimum, maximum and interquartile range of the ms of client CPU
per MiB of batch body, then the A/A gap. Exits 1 when a run of the harness fails, which includes an
integrity failure of the received data.
USAGE
}

MODE=bytes
ROUNDS=20
HUB_PORT=5352
BUILD_HUB=true
WITNESS=true
HARNESS_ARGS=()
ARM_LABELS=()
ARM_DIRS=()

while [ $# -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --mode) MODE="${2:-}"; shift 2 ;;
        --rounds) ROUNDS="${2:-}"; shift 2 ;;
        --megabytes|--warmup-megabytes|--seed) HARNESS_ARGS+=("$1" "${2:-}"); shift 2 ;;
        --hub-port) HUB_PORT="${2:-}"; shift 2 ;;
        --no-aa) WITNESS=false; shift ;;
        --no-hub-build) BUILD_HUB=false; shift ;;
        -*) log_error "Unknown option $1"; usage >&2; exit 2 ;;
        *)
            case "$1" in
                *=*) label="${1%%=*}"; dir="${1#*=}" ;;
                *) dir="$1"; label="$(basename "$1")" ;;
            esac
            ARM_LABELS+=("$label")
            ARM_DIRS+=("$dir")
            shift
            ;;
    esac
done

case "$MODE" in items|bytes) ;; *) log_error "--mode is items or bytes"; exit 2 ;; esac
case "$ROUNDS" in ''|*[!0-9]*|0) log_error "--rounds needs a positive integer"; exit 2 ;; esac
case "$HUB_PORT" in ''|*[!0-9]*) log_error "--hub-port needs an integer"; exit 2 ;; esac
if [ ${#ARM_DIRS[@]} -eq 0 ]; then
    log_error "At least one arm is needed."
    usage >&2
    exit 2
fi

# The A/A witness is the first arm again; the runs table below is keyed by label.
if $WITNESS; then
    ARM_LABELS+=("${ARM_LABELS[0]}-aa")
    ARM_DIRS+=("${ARM_DIRS[0]}")
fi

for i in "${!ARM_DIRS[@]}"; do
    if ! printf '%s' "${ARM_LABELS[$i]}" | grep -Eq '^[A-Za-z0-9._+-]+$'; then
        log_error "Label '${ARM_LABELS[$i]}' must be letters, digits and . _ + - only."
        exit 2
    fi
    if [ ! -f "${ARM_DIRS[$i]}/TransportR.ClientBench.dll" ]; then
        log_error "No harness build in '${ARM_DIRS[$i]}' (expected TransportR.ClientBench.dll)."
        exit 2
    fi
    for j in "${!ARM_LABELS[@]}"; do
        if [ "$j" -lt "$i" ] && [ "${ARM_LABELS[$j]}" = "${ARM_LABELS[$i]}" ]; then
            log_error "Label '${ARM_LABELS[$i]}' is used twice."
            exit 2
        fi
    done
done

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/client-bench.XXXXXX")"
HUB_PID=""

cleanup() {
    if [ -n "$HUB_PID" ]; then
        kill "$HUB_PID" 2>/dev/null || true
        wait "$HUB_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if $BUILD_HUB; then
    log_step "Building the hub once..."
    if ! dotnet build "$PROJECT_ROOT/samples/DataHub/DataHub.csproj" -c Release --nologo -v quiet > "$WORK_DIR/hub-build.log" 2>&1; then
        cat "$WORK_DIR/hub-build.log"
        log_error "The hub did not build."
        exit 1
    fi
fi

HUB_DLL="$PROJECT_ROOT/samples/DataHub/bin/Release/net10.0/DataHub.dll"
if [ ! -f "$HUB_DLL" ]; then
    log_error "No hub build at $HUB_DLL."
    exit 1
fi

# Unlike the scenario runner, this does not free a busy port: whatever holds it may be someone's hub.
if lsof -ti:"$HUB_PORT" >/dev/null 2>&1; then
    log_error "Port $HUB_PORT is in use; pick another with --hub-port."
    exit 1
fi

log_step "Starting the hub on port $HUB_PORT..."
mkdir -p "$WORK_DIR/hub"
# The hub is configured from its environment: ambient settings of a scenario or a shell must not leak in.
env -u USE_REDIS -u USE_TEST_PROXY -u FLOWCONTROL_MODE -u EnableAuth -u EnableEmbeddedIdp \
    OUTPUT_DIR="$WORK_DIR/hub" HUB_PORT="$HUB_PORT" ASPNETCORE_URLS="http://localhost:$HUB_PORT" \
    dotnet "$HUB_DLL" > "$WORK_DIR/hub/stdout.log" 2>&1 &
HUB_PID=$!
if ! wait_for_url "http://localhost:$HUB_PORT/health" "Hub"; then
    cat "$WORK_DIR/hub/stdout.log"
    log_error "The hub did not answer."
    exit 1
fi

log_info "mode=$MODE rounds=$ROUNDS arms=${ARM_LABELS[*]}"
log_info "machine: $(uname -sm), load before: $(uptime | sed 's/.*load averages*: //')"

RUNS="$WORK_DIR/runs.txt"
: > "$RUNS"
ARM_COUNT=${#ARM_DIRS[@]}

run_arm() {
    local round=$1 index=$2
    local label="${ARM_LABELS[$index]}" dir="${ARM_DIRS[$index]}"
    local out="$WORK_DIR/run.out" err="$WORK_DIR/run.err"

    if ! dotnet "$dir/TransportR.ClientBench.dll" --hub "http://localhost:$HUB_PORT" --mode "$MODE" \
            ${HARNESS_ARGS[@]+"${HARNESS_ARGS[@]}"} > "$out" 2> "$err"; then
        cat "$out" "$err"
        log_error "Round $round, arm $label: the harness failed."
        return 1
    fi

    local result
    result=$(grep '^RESULT ' "$out" | head -n 1)
    local value
    value=$(printf '%s' "$result" | sed -n 's/.* ms_per_mb=\([0-9.]*\).*/\1/p')
    if [ -z "$value" ]; then
        cat "$out" "$err"
        log_error "Round $round, arm $label: no result line."
        return 1
    fi

    echo "$label $value" >> "$RUNS"
    echo "round $round  $label  $result"
    grep '^# gc collections' "$out" | sed 's/^/                /'
}

for ((round = 1; round <= ROUNDS; round++)); do
    for ((k = 0; k < ARM_COUNT; k++)); do
        # Reversed on even rounds, so that no arm always runs first or last.
        if [ $((round % 2)) -eq 1 ]; then index=$k; else index=$((ARM_COUNT - 1 - k)); fi
        run_arm "$round" "$index" || exit 1
    done
done

# Quartiles by linear interpolation between the order statistics.
stats() {
    sort -g | awk '
        { v[NR] = $1; sum += $1 }
        function q(p,   h, lo) { h = (NR - 1) * p + 1; lo = int(h); return v[lo] + (h - lo) * ((lo < NR ? v[lo + 1] : v[lo]) - v[lo]) }
        END {
            printf "%d %.4f %.4f %.4f %.4f %.4f\n", NR, q(0.5), sum / NR, v[1], v[NR], q(0.75) - q(0.25)
        }'
}

echo ""
echo "ms of client CPU per MiB of batch body (mode=$MODE, $ROUNDS rounds)"
printf '%-22s %4s %9s %9s %9s %9s %9s\n' arm n median mean min max iqr
for i in "${!ARM_LABELS[@]}"; do
    label="${ARM_LABELS[$i]}"
    row=$(awk -v l="$label" '$1 == l { print $2 }' "$RUNS" | stats)
    # shellcheck disable=SC2086
    printf '%-22s %4s %9s %9s %9s %9s %9s\n' "$label" $row
    echo "$label $row" >> "$WORK_DIR/stats.txt"
done

echo ""
first="${ARM_LABELS[0]}"
first_median=$(awk -v l="$first" '$1 == l { print $3 }' "$WORK_DIR/stats.txt")
first_mean=$(awk -v l="$first" '$1 == l { print $4 }' "$WORK_DIR/stats.txt")
if $WITNESS; then
    witness="${first}-aa"
    witness_median=$(awk -v l="$witness" '$1 == l { print $3 }' "$WORK_DIR/stats.txt")
    witness_mean=$(awk -v l="$witness" '$1 == l { print $4 }' "$WORK_DIR/stats.txt")
    awk -v a="$first_median" -v b="$witness_median" -v ma="$first_mean" -v mb="$witness_mean" -v f="$first" -v w="$witness" 'BEGIN {
        printf "A/A gap, %s against %s: %.2f%% on the medians, %.2f%% on the means\n", f, w, 100 * (b - a) / a, 100 * (mb - ma) / ma
    }'
fi
for i in "${!ARM_LABELS[@]}"; do
    label="${ARM_LABELS[$i]}"
    [ "$label" = "$first" ] && continue
    [ "$label" = "${first}-aa" ] && continue
    median=$(awk -v l="$label" '$1 == l { print $3 }' "$WORK_DIR/stats.txt")
    awk -v a="$first_median" -v b="$median" -v f="$first" -v l="$label" 'BEGIN {
        printf "%s against %s: %+.2f%% on the medians\n", l, f, 100 * (b - a) / a
    }'
done
echo "Read a difference between arms only where it exceeds the A/A gap and the interquartile ranges."
echo "load after: $(uptime | sed 's/.*load averages*: //')"
