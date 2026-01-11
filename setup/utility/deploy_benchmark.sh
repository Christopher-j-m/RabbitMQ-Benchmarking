#!/bin/bash
# Deploy benchmark tool to load generator VM

# Params
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.txt"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
BENCHMARK_DIR="$SCRIPT_DIR/../../benchmark"
BINARY_NAME="rmq-benchmark"

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

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
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

print_info "Deploying benchmarking tool to $LOAD_GENERATOR_IP..."

# Build the Go binary
if [[ ! -d "$BENCHMARK_DIR" ]]; then
    print_error "Benchmark directory not found at: $BENCHMARK_DIR"
    exit 1
fi

cd "$BENCHMARK_DIR"
if ! go build -o "$BINARY_NAME" .; then
    print_error "Failed to build Go binary"
    exit 1
fi
cd - > /dev/null

REMOTE_DIR="/home/$REMOTE_USER/benchmarking"

# Test SSH connection
if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$LOAD_GENERATOR_IP" "echo 'SSH connection successful'" &>/dev/null; then
    print_error "Failed to connect to $LOAD_GENERATOR_IP via SSH"
    exit 1
fi

# Create remote directory
ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$LOAD_GENERATOR_IP" "mkdir -p $REMOTE_DIR"

# Copy binary
if [[ -f "$BENCHMARK_DIR/$BINARY_NAME" ]]; then
    if ! scp -i "$SSH_KEY_PATH" $SSH_OPTS "$BENCHMARK_DIR/$BINARY_NAME" "$REMOTE_USER@$LOAD_GENERATOR_IP:$REMOTE_DIR/" &>/dev/null; then
        print_error "Failed to copy benchmark binary to $LOAD_GENERATOR_IP"
        exit 1
    fi
else
    print_error "Binary not found at $BENCHMARK_DIR/$BINARY_NAME"
    exit 1
fi

# Make binary executable
ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$LOAD_GENERATOR_IP" "chmod +x $REMOTE_DIR/$BINARY_NAME"

print_info "Benchmark tool copied to load generator VM"