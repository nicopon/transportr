#!/bin/bash
# scripts/init.sh - Initialize environment for benchmarks
#
# Usage: ./scripts/init.sh [options]
#
# Options:
#   --skip-data   Skip data generation if orders.bin already exists
#
# Examples:
#   ./scripts/init.sh              # full initialization
#   ./scripts/init.sh --skip-data  # skip data generation
#
# This script exits non-zero as soon as something it attempted fails. Callers
# judge it on that: test-all.sh treats a failed init as a failed run, which is
# only meaningful if a failure can actually reach the exit code.

set -e

# Determine script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Default configuration
DEFAULT_ITEM_COUNT=2000000
DEFAULT_LOG_LEVEL="Warning"
TOXIPROXY_VERSION="2.7.0"
REDIS_VERSION="7.2.4"
DATA_FILE="data/orders.bin"

# Parse arguments
SKIP_DATA=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-data)
            SKIP_DATA=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Parameters (environment variables or defaults)
ITEM_COUNT=${ITEM_COUNT:-$DEFAULT_ITEM_COUNT}
LOG_LEVEL=${LOG_LEVEL:-$DEFAULT_LOG_LEVEL}

echo "=================================================="
echo "Environment initialization with:"
echo "  - Items       : $ITEM_COUNT"
echo "  - Log Level   : $LOG_LEVEL"
echo "  - Skip Data   : $SKIP_DATA"
echo "=================================================="

# 0. Environment setup (Toxiproxy & Redis)
echo "Checking tools..."
mkdir -p tools

# OS and architecture detection.
#
# Prints the platform on stdout. It cannot report failure by exiting: it runs
# inside a command substitution, where exit ends the subshell and leaves the
# caller running with the error message as the platform string. It returns
# instead, and the caller checks.
detect_platform() {
    local os arch
    case "$(uname -s)" in
        Darwin) os="darwin" ;;
        Linux)  os="linux" ;;
        *)      echo "Unsupported OS: $(uname -s)" >&2; return 1 ;;
    esac

    case "$(uname -m)" in
        x86_64)  arch="amd64" ;;
        amd64)   arch="amd64" ;;
        arm64)   arch="arm64" ;;
        aarch64) arch="arm64" ;;
        *)       echo "Unsupported architecture: $(uname -m)" >&2; return 1 ;;
    esac

    echo "${os}-${arch}"
}

if ! PLATFORM=$(detect_platform); then
    echo "ERROR: cannot initialize on this platform."
    exit 1
fi
echo "Detected platform: $PLATFORM"

# Download a tool and make it executable.
#
# -f so an HTTP error is a curl failure rather than a 200-shaped error page
# written to the target: without it a 404 leaves a few hundred bytes of HTML
# marked executable, and the failure only surfaces later as a tool that will
# not start.
download_tool() {
    local url=$1
    local target=$2
    local name

    name="$(basename "$target")"
    echo "Downloading $name (${PLATFORM})..."
    if ! curl -fL --retry 2 -o "$target" "$url"; then
        echo "ERROR: failed to download $name from $url"
        rm -f "$target"
        return 1
    fi
    chmod +x "$target"
}

# Download Toxiproxy if needed
TOXIPROXY_BASE_URL="https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY_VERSION}"

if [ ! -f "tools/toxiproxy-server" ]; then
    download_tool "${TOXIPROXY_BASE_URL}/toxiproxy-server-${PLATFORM}" "tools/toxiproxy-server"
fi

if [ ! -f "tools/toxiproxy-cli" ]; then
    download_tool "${TOXIPROXY_BASE_URL}/toxiproxy-cli-${PLATFORM}" "tools/toxiproxy-cli"
fi

# Redis.
#
# This script does not provision redis, it reports where the benchmark will
# find one. Only linux-amd64 has a static build to fetch; on macOS there is no
# arm64 binary to download, and the scenario falls back to a container at run
# time (scripts/lib/redis.sh). Reporting rather than warning-and-continuing is
# the point: ha-redis is the only scenario that needs this, so an absence is
# not a reason to fail the whole initialization — but it must be legible.
if [ "$PLATFORM" == "linux-amd64" ] && [ ! -f "tools/redis-server" ]; then
    download_tool \
        "https://github.com/phlummox/redis-static-binaries/releases/download/v${REDIS_VERSION}/redis-server-v${REDIS_VERSION}-x86_64" \
        "tools/redis-server"
fi

if [ -f "tools/redis-server" ] || command -v redis-server >/dev/null 2>&1 \
   || [ -f "/opt/homebrew/bin/redis-server" ]; then
    echo "Redis          : native binary"
elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "Redis          : none installed, ha-redis will use a container"
else
    echo "Redis          : UNAVAILABLE - ha-redis cannot run."
    echo "                 Install redis (brew install redis) or start Docker."
fi

# 1. Database initialization
if [ "$SKIP_DATA" = "true" ] && [ -f "$DATA_FILE" ]; then
    echo "Skipping data generation (--skip-data, file exists: $DATA_FILE)"
else
    echo "Starting DatabaseInitializer..."
    export ITEM_COUNT=$ITEM_COUNT
    export LOG_LEVEL=$LOG_LEVEL
    dotnet run --project samples/DatabaseInitializer/DatabaseInitializer.csproj

    if [ ! -s "$DATA_FILE" ]; then
        echo "ERROR: DatabaseInitializer reported success but $DATA_FILE is missing or empty."
        exit 1
    fi
fi

echo "Initialization complete."
