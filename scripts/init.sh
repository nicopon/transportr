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

# OS and architecture detection
detect_platform() {
    local os arch
    case "$(uname -s)" in
        Darwin) os="darwin" ;;
        Linux)  os="linux" ;;
        *)      echo "Unsupported OS: $(uname -s)"; exit 1 ;;
    esac
    
    case "$(uname -m)" in
        x86_64)  arch="amd64" ;;
        amd64)   arch="amd64" ;;
        arm64)   arch="arm64" ;;
        aarch64) arch="arm64" ;;
        *)       echo "Unsupported architecture: $(uname -m)"; exit 1 ;;
    esac
    
    echo "${os}-${arch}"
}

PLATFORM=$(detect_platform)
echo "Detected platform: $PLATFORM"

# Download Toxiproxy if needed
TOXIPROXY_BASE_URL="https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY_VERSION}"

if [ ! -f "tools/toxiproxy-server" ]; then
    echo "Downloading toxiproxy-server (${PLATFORM})..."
    curl -L -o tools/toxiproxy-server "${TOXIPROXY_BASE_URL}/toxiproxy-server-${PLATFORM}"
    chmod +x tools/toxiproxy-server
fi

if [ ! -f "tools/toxiproxy-cli" ]; then
    echo "Downloading toxiproxy-cli (${PLATFORM})..."
    curl -L -o tools/toxiproxy-cli "${TOXIPROXY_BASE_URL}/toxiproxy-cli-${PLATFORM}"
    chmod +x tools/toxiproxy-cli
fi

# Download Redis if needed
if [ "$PLATFORM" == "linux-amd64" ]; then
    REDIS_URL="https://github.com/phlummox/redis-static-binaries/releases/download/v${REDIS_VERSION}/redis-server-v${REDIS_VERSION}-x86_64"
    if [ ! -f "tools/redis-server" ]; then
        echo "Downloading redis-server (${PLATFORM})..."
        curl -L -o tools/redis-server "$REDIS_URL"
        chmod +x tools/redis-server
    fi
elif [ "$PLATFORM" == "darwin-amd64" ] || [ "$PLATFORM" == "darwin-arm64" ]; then
    # On macOS, we rely on brew or system install, but we can symlink it if found
    if [ ! -f "tools/redis-server" ]; then
        if command -v redis-server >/dev/null 2>&1; then
            echo "Found system redis-server, symlinking..."
            ln -sf "$(command -v redis-server)" tools/redis-server
        elif [ -f "/opt/homebrew/bin/redis-server" ]; then
             echo "Found brew redis-server, symlinking..."
             ln -sf "/opt/homebrew/bin/redis-server" tools/redis-server
        else
            echo "WARNING: redis-server not found. Please install it (brew install redis)."
        fi
    fi
fi

# 1. Database initialization
if [ "$SKIP_DATA" = "true" ] && [ -f "$DATA_FILE" ]; then
    echo "Skipping data generation (--skip-data, file exists: $DATA_FILE)"
else
    echo "Starting DatabaseInitializer..."
    export ITEM_COUNT=$ITEM_COUNT
    export LOG_LEVEL=$LOG_LEVEL
    dotnet run --project samples/DatabaseInitializer/DatabaseInitializer.csproj
fi

echo "Initialization complete."