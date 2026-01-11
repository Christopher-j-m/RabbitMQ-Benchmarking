#!/bin/bash
# Removes all queues from the RabbitMQ cluster using the Management API

# Params
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.txt"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check config
if [[ ! -f "$CONFIG_FILE" ]]; then
    print_error "Configuration file '$CONFIG_FILE' not found!"
    exit 1
fi

source "$CONFIG_FILE"

# Check required variables
if [[ -z "$CLUSTER_NODE_IPS" ]]; then
    print_error "CLUSTER_NODE_IPS not found in config.txt"
    exit 1
fi

if [[ -z "$RABBITMQ_ADMIN_USER" ]] || [[ -z "$RABBITMQ_ADMIN_PASSWORD" ]]; then
    print_error "RabbitMQ credentials not found in config.txt"
    exit 1
fi

# Parse cluster node IPs
IFS=',' read -ra NODE_IPS <<< "$CLUSTER_NODE_IPS"
for i in "${!NODE_IPS[@]}"; do
    NODE_IPS[$i]=$(echo "${NODE_IPS[$i]}" | xargs)
done

# Find a reachable node
MANAGEMENT_URL=""
for IP in "${NODE_IPS[@]}"; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "$RABBITMQ_ADMIN_USER:$RABBITMQ_ADMIN_PASSWORD" "http://$IP:15672/api/overview" --connect-timeout 5)
    
    if [[ "$HTTP_CODE" == "200" ]]; then
        MANAGEMENT_URL="http://$IP:15672"
        break
    fi
done

if [[ -z "$MANAGEMENT_URL" ]]; then
    print_error "Could not connect to any RabbitMQ node Management API."
    exit 1
fi

# List queues
QUEUES_JSON=$(curl -s -u "$RABBITMQ_ADMIN_USER:$RABBITMQ_ADMIN_PASSWORD" "$MANAGEMENT_URL/api/queues")

if command -v jq &> /dev/null; then
    TARGET_QUEUES=$(echo "$QUEUES_JSON" | jq -r ".[].name")
else
    TARGET_QUEUES=$(echo "$QUEUES_JSON" | grep -o '"name":"[^"]*"' | cut -d'"' -f4)
fi

if [[ -z "$TARGET_QUEUES" ]]; then
    print_info "No queues found to delete."
    exit 0
fi

# Delete queues
for Q in $TARGET_QUEUES; do
    VHOST="%2f"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE -u "$RABBITMQ_ADMIN_USER:$RABBITMQ_ADMIN_PASSWORD" "$MANAGEMENT_URL/api/queues/$VHOST/$Q")
    
    if [[ "$HTTP_CODE" != "204" ]] && [[ "$HTTP_CODE" != "200" ]]; then
        print_error "Failed to delete queue $Q (HTTP $HTTP_CODE)"
    fi
done

print_info "All queues deleted"