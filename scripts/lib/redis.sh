#!/bin/bash
# scripts/lib/redis.sh - Redis server management
#
# This library provides functions to start and stop Redis:
# - start_redis [port]
# - stop_redis
# - wait_for_redis [port]
#
# Redis comes from a native binary when there is one, and from a container
# otherwise. There is no static redis-server build for arm64 macOS, so on that
# platform the container is the only route that does not ask the developer to
# install redis system-wide. Docker is already a project dependency: the
# RedisStateStoreTests run on Testcontainers.

REDIS_PORT=${REDIS_PORT:-6379}
REDIS_PID=""
REDIS_CONTAINER=""
REDIS_IMAGE=${REDIS_IMAGE:-redis:7-alpine}

# Resolve a native redis-server, printing its path. Non-zero when there is none.
find_redis_binary() {
    if [ -f "$PROJECT_ROOT/tools/redis-server" ]; then
        echo "$PROJECT_ROOT/tools/redis-server"
    elif command -v redis-server >/dev/null 2>&1; then
        command -v redis-server
    elif [ -f "/opt/homebrew/bin/redis-server" ]; then
        echo "/opt/homebrew/bin/redis-server"
    else
        return 1
    fi
}

# Start Redis server
start_redis() {
    local port=${1:-$REDIS_PORT}
    log_step "Starting Redis on port $port..."

    # Kill any existing redis
    pkill -f "redis-server .*:$port" 2>/dev/null || true
    sleep 1

    local redis_bin=""
    if redis_bin="$(find_redis_binary)"; then
        # --protected-mode no for development ease, --save "" to disable RDB
        # persistence (no dump.rdb left behind).
        "$redis_bin" --port "$port" --protected-mode no --save "" --daemonize no --loglevel notice > "${OUTPUT_DIR}/redis.log" 2>&1 &
        REDIS_PID=$!
        register_pid $REDIS_PID
        wait_for_redis "$port" || return 1
        log_info "Redis started from $redis_bin (PID: $REDIS_PID)"
    elif docker_available; then
        REDIS_CONTAINER="transportr-bench-redis-$port"
        log_info "No native redis-server; starting $REDIS_IMAGE in a container."
        docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
        if ! docker run -d --rm --name "$REDIS_CONTAINER" -p "${port}:6379" "$REDIS_IMAGE" \
                redis-server --protected-mode no --save "" > "${OUTPUT_DIR}/redis.log" 2>&1; then
            log_error "Failed to start the $REDIS_IMAGE container. See ${OUTPUT_DIR}/redis.log."
            REDIS_CONTAINER=""
            return 1
        fi
        wait_for_redis "$port" || return 1
        log_info "Redis started in container $REDIS_CONTAINER"
    else
        log_error "redis-server not found and no usable Docker daemon."
        log_error "Install redis (brew install redis) or start Docker, then rerun."
        return 1
    fi
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
    if [ -n "$REDIS_CONTAINER" ]; then
        docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
        REDIS_CONTAINER=""
    fi
    if [ -n "$REDIS_PID" ]; then
        kill $REDIS_PID 2>/dev/null || true
    fi
    pkill -f "redis-server .*:$REDIS_PORT" 2>/dev/null || true
}
