#!/bin/bash
# scripts/lib/idp.sh - TestIdp management for OIDC scenarios

IDP_PORT=${IDP_PORT:-5100}
IDP_URL="http://localhost:$IDP_PORT"
IDP_PID=""

# Start TestIdp
start_idp() {
    log_step "Starting TestIdp on port $IDP_PORT..."
    pkill -f "TestIdp.dll" 2>/dev/null
    sleep 1
    
    # Build if needed
    dotnet build samples/TestIdp -c Release > /dev/null 2>&1
    
    # Ensure output directory exists and get absolute path
    local output_dir="${OUTPUT_DIR:-output}"
    mkdir -p "$output_dir"
    local abs_output_dir="$(cd "$output_dir" && pwd)"
    
    # Start IDP
    cd samples/TestIdp/bin/Release/net10.0
    ASPNETCORE_URLS="$IDP_URL" dotnet TestIdp.dll > "$abs_output_dir/idp.log" 2>&1 &
    IDP_PID=$!
    cd - > /dev/null
    
    register_pid $IDP_PID
    wait_for_url "$IDP_URL/health" "IDP" || return 1
}

# Stop TestIdp
stop_idp() {
    if [ -n "$IDP_PID" ]; then
        kill $IDP_PID 2>/dev/null || true
    fi
    pkill -f "TestIdp.dll" 2>/dev/null || true
}

# Get access token from IDP
get_token() {
    local client_id=$1
    local client_secret=$2
    local scope=$3
    
    # Token response may have newlines, flatten with tr before extraction
    local response=$(curl -s -X POST "$IDP_URL/connect/token" \
        -d "grant_type=client_credentials&client_id=$client_id&client_secret=$client_secret&scope=$scope" \
        | tr -d '\n ')
    
    echo "$response" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'
}

# Acquire tokens for sender and receiver
acquire_tokens() {
    log_step "Acquiring tokens..."
    
    export SENDER_TOKEN=$(get_token "sender" "sender-secret" "transportr:group:senders")
    export RECEIVER_TOKEN=$(get_token "receiver" "receiver-secret" "transportr:group:receivers")
    
    if [ -z "$SENDER_TOKEN" ] || [ -z "$RECEIVER_TOKEN" ]; then
        log_error "Failed to acquire tokens"
        return 1
    fi
    
    log_info "Tokens acquired successfully"
    
    # Export for clients
    export Sender__AccessToken="$SENDER_TOKEN"
    export Receiver__AccessToken="$RECEIVER_TOKEN"
}

# Acquire tokens from embedded IDP (Hub itself)
acquire_tokens_from_hub() {
    log_step "Acquiring tokens from embedded IDP..."
    
    local hub_base_url="http://localhost:${HUB_PORT:-5252}"
    
    export SENDER_TOKEN=$(get_token_from_url "$hub_base_url" "sender" "sender-secret" "transportr:group:senders")
    export RECEIVER_TOKEN=$(get_token_from_url "$hub_base_url" "receiver" "receiver-secret" "transportr:group:receivers")
    
    if [ -z "$SENDER_TOKEN" ] || [ -z "$RECEIVER_TOKEN" ]; then
        log_error "Failed to acquire tokens from embedded IDP"
        return 1
    fi
    
    log_info "Tokens acquired from embedded IDP"
    
    # Export for clients
    export Sender__AccessToken="$SENDER_TOKEN"
    export Receiver__AccessToken="$RECEIVER_TOKEN"
}

# Get access token from a specific URL
get_token_from_url() {
    local base_url=$1
    local client_id=$2
    local client_secret=$3
    local scope=$4
    
    local response=$(curl -s -X POST "$base_url/connect/token" \
        -d "grant_type=client_credentials&client_id=$client_id&client_secret=$client_secret&scope=$scope" \
        | tr -d '\n ')
    
    echo "$response" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'
}
