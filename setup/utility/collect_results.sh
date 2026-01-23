#!/bin/bash
# Collect benchmark results from load generator VM

# Params
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.txt"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
LOCAL_RESULTS_DIR="$SCRIPT_DIR/../../results"

# Format out
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

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    print_error "Configuration file '$CONFIG_FILE' not found!"
    print_info "Please run 'terraform apply' first to generate the config file."
    exit 1
fi

source "$CONFIG_FILE"

# Validate required variables
if [[ -z "$SSH_KEY_PATH" ]]; then
    print_error "SSH_KEY_PATH is not set in $CONFIG_FILE"
    exit 1
fi

if [[ -z "$LOAD_GENERATOR_IP" ]]; then
    print_error "LOAD_GENERATOR_IP is not set in $CONFIG_FILE"
    exit 1
fi

if [[ -z "$REMOTE_USER" ]]; then
    print_error "REMOTE_USER is not set in $CONFIG_FILE"
    exit 1
fi

if [[ ! -f "$SSH_KEY_PATH" ]]; then
    print_error "SSH key not found at: $SSH_KEY_PATH"
    exit 1
fi

print_info "Collecting benchmark results from $LOAD_GENERATOR_IP..."

# Check if rsync is available
if ! command -v rsync &>/dev/null; then
    print_error "rsync is not installed. Please install it first."
    exit 1
fi

# Create local results directory if it doesn't exist
mkdir -p "$LOCAL_RESULTS_DIR"

REMOTE_RESULTS_DIR="/home/$REMOTE_USER/benchmarking/results"

# Test SSH connection
if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$LOAD_GENERATOR_IP" "echo 'SSH connection successful'" &>/dev/null; then
    print_error "Failed to connect to $LOAD_GENERATOR_IP via SSH"
    exit 1
fi

# Check if remote results directory exists
if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$LOAD_GENERATOR_IP" "test -d $REMOTE_RESULTS_DIR" &>/dev/null; then
    print_warn "Results directory not found at $REMOTE_RESULTS_DIR on $LOAD_GENERATOR_IP"
    exit 0
fi

# Copy results using rsync
if rsync -avz -e "ssh -i $SSH_KEY_PATH $SSH_OPTS" \
    "$REMOTE_USER@$LOAD_GENERATOR_IP:$REMOTE_RESULTS_DIR/" "$LOCAL_RESULTS_DIR/" &>/dev/null; then
    print_info "Results collected successfully!"
else
    print_error "Failed to copy results using rsync"
    exit 1
fi
