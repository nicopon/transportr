#!/bin/bash
# scripts/lib/nginx.sh - nginx in a container, in front of a single hub
#
# This library provides functions to start and stop that nginx:
# - start_nginx
# - stop_nginx
#
# The configuration is the bare minimum for a reverse proxy: scripts/nginx/default.conf.template
# sets nothing but a listener and a proxy_pass, so every other behaviour is the stock one. That is
# the point: it is what a deployment gets before anyone tunes the proxy.
#
# The container reaches the hub on the host through host.docker.internal, so the hub must answer on
# the host's loopback interface.

NGINX_PORT=${NGINX_PORT:-8088}
NGINX_CONTAINER=""
NGINX_IMAGE=${NGINX_IMAGE:-nginx:1.31.7-alpine}

# Start nginx and send the clients through it
start_nginx() {
    if ! docker_available; then
        log_error "A container runtime is required for this scenario, and no Docker daemon answers."
        return 1
    fi

    log_step "Starting $NGINX_IMAGE on port $NGINX_PORT..."
    local name="transportr-bench-nginx-$NGINX_PORT"
    docker rm -f "$name" >/dev/null 2>&1 || true
    ensure_port_free "$NGINX_PORT" || return 1

    local conf="$OUTPUT_DIR/nginx.conf"
    sed "s/\${HUB_PORT}/$HUB_PORT/" "$SCRIPT_DIR/nginx/default.conf.template" > "$conf"
    local conf_dir
    conf_dir="$(cd "$(dirname "$conf")" && pwd)"

    NGINX_CONTAINER="$name"
    if ! docker run -d --rm --name "$name" -p "${NGINX_PORT}:80" \
            --add-host host.docker.internal:host-gateway \
            -v "$conf_dir/nginx.conf:/etc/nginx/nginx.conf:ro" "$NGINX_IMAGE" > "$OUTPUT_DIR/nginx-start.log" 2>&1; then
        log_error "Failed to start the $NGINX_IMAGE container. See $OUTPUT_DIR/nginx-start.log."
        NGINX_CONTAINER=""
        return 1
    fi

    wait_for_url "http://localhost:$NGINX_PORT/health" "nginx" || return 1
    log_info "nginx started in container $NGINX_CONTAINER"

    export HUB_URL="http://localhost:$NGINX_PORT"
    export Sender__HubUrl="$HUB_URL"
    export Receiver__HubUrl="$HUB_URL"
    log_info "Through nginx (port $NGINX_PORT)"
}

# Stop nginx, keeping what it logged
stop_nginx() {
    if [ -n "$NGINX_CONTAINER" ]; then
        docker logs "$NGINX_CONTAINER" > "$OUTPUT_DIR/nginx.log" 2>&1 || true
        docker rm -f "$NGINX_CONTAINER" >/dev/null 2>&1 || true
        NGINX_CONTAINER=""
    fi
}
