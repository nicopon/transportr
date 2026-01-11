#!/bin/bash
# scripts/lib/redis.sh - Redis server management
#
# This library provides functions to start and stop Redis:
# - start_redis [port]
# - stop_redis
# - wait_for_redis [port]

REDIS_PORT=${REDIS_PORT:-6379}
REDIS_PID=""

# Start Redis server
start_redis() {
    local port=${1:-$REDIS_PORT}
    log_step "Starting Redis on port $port..."
    
    # Kill any existing redis
    pkill -f "redis-server .*:$port" 2>/dev/null || true
    sleep 1
    
    # Determine redis-server path
    local redis_bin=""
    if [ -f "$PROJECT_ROOT/tools/redis-server" ]; then
        redis_bin="$PROJECT_ROOT/tools/redis-server"
    elif command -v redis-server >/dev/null 2>&1; then
        redis_bin="$(command -v redis-server)"
    elif [ -f "/opt/homebrew/bin/redis-server" ]; then
        redis_bin="/opt/homebrew/bin/redis-server"
    else
        log_error "redis-server not found. Please run ./scripts/init.sh or install redis."
        return 1
    fi

    # Start redis server in background
    # We use --port and --protected-mode no for development ease
    # We use --save "" to disable RDB persistence (no dump.rdb)
    "$redis_bin" --port "$port" --protected-mode no --save "" --daemonize no --loglevel notice > "${OUTPUT_DIR}/redis.log" 2>&1 &
    REDIS_PID=$!
    register_pid $REDIS_PID
    
    # Wait for redis to be ready
    wait_for_redis "$port"
    
    log_info "Redis started (PID: $REDIS_PID)"
}

# Wait for Redis to be ready
wait_for_redis() {
    local port=${1:-$REDIS_PORT}
    local retries=0
    
    log_info "Waiting for Redis on port $port..."
    
    while ! nc -z localhost "$port"; do
        sleep 0.5
        retries=$((retries + 1))
        if [ $retries -gt 20 ]; then
            log_error "Redis failed to start on port $port"
            return 1
        fi
    done
    
    # Optional: Send PING? simplified check via nc/port open is usually enough for local dev
}

# Stop Redis
stop_redis() {
    if [ -n "$REDIS_PID" ]; then
        kill $REDIS_PID 2>/dev/null || true
    fi
    pkill -f "redis-server .*:$REDIS_PORT" 2>/dev/null || true
}
