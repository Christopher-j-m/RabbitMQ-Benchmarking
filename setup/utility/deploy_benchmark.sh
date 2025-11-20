#!/bin/bash
# Deploy benchmark tool to load generator VM(s)

# Params
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.txt"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
FILES_TO_COPY=(
    "benchmark_runner.py"
    "config.yaml"
    "requirements.txt"
    "common/"
    "experiments/"
)

# Format out, adapted from: https://labex.io/tutorials/shell-how-to-format-strings-in-bash-scripts-400162#adding-color-and-style-to-bash-output
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

if [[ -z "$LOAD_GENERATOR_IPS" ]]; then
    print_error "LOAD_GENERATOR_IPS is not set in $CONFIG_FILE"
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

# Parse VM names into array & trim whitespaces
# Adapted from: https://unix.stackexchange.com/questions/184863/
IFS=',' read -ra VM_ARRAY <<< "$LOAD_GENERATOR_IPS"
for i in "${!VM_ARRAY[@]}"; do
    VM_ARRAY[$i]=$(echo "${VM_ARRAY[$i]}" | xargs)
done

print_info "Starting deployment of benchmarking tool to ${#VM_ARRAY[@]} load generator VM(s)..."
print_info "SSH Key-Path: $SSH_KEY_PATH"
print_info "User: $REMOTE_USER"
echo ""

REMOTE_DIR="/home/$REMOTE_USER/benchmarking"

deploy_to_vm() {
    local ip=$1
    local vm_index=$2
    
    print_info "[$vm_index] Deploying to $REMOTE_USER@$ip..."
    
    # Test SSH connection
    if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" "echo 'SSH connection successful'" &>/dev/null; then
        print_error "[$vm_index] Failed to connect to $ip via SSH"
        return 1
    fi
    
    # Create remote directory
    print_info "[$vm_index] Creating remote directory: $REMOTE_DIR"
    ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" "mkdir -p $REMOTE_DIR"
    
    # Copy files (only diffs => faster for repeated deployments)
    print_info "[$vm_index] Copying files..."
    for item in "${FILES_TO_COPY[@]}"; do
        if [[ -e "$SCRIPT_DIR/../../$item" ]]; then
            print_info "[$vm_index]   -> $item"
            rsync -az --relative -e "ssh -i $SSH_KEY_PATH $SSH_OPTS" \
                --exclude='__pycache__' \
                --exclude='*.pyc' \
                --exclude='.git' \
                --exclude='results/' \
                --exclude='.venv/' \
                --exclude='venv/' \
                "$SCRIPT_DIR/../.././$item" "$REMOTE_USER@$ip:$REMOTE_DIR/"
        else
            print_warn "[$vm_index]   -> $item not found, skipping"
        fi
    done
    
    # Create venv and install dependencies from requirements.txt
    print_info "[$vm_index] Creating virtual environment..."
    if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" \
        "cd $REMOTE_DIR && python3 -m venv .venv" 2>/dev/null; then
        print_warn "[$vm_index] Failed to create virtual environment"
    else
        print_info "[$vm_index] Installing Python dependencies..."
        if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" \
            "cd $REMOTE_DIR && source .venv/bin/activate && pip install -q -r requirements.txt" 2>/dev/null; then
            print_warn "[$vm_index] Failed to install dependencies"
        fi
    fi
    
    # Make scripts executable
    ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" \
        "chmod +x $REMOTE_DIR/*.py 2>/dev/null || true"
    
    print_info "[$vm_index] Deployment to $ip completed successfully!"
    return 0
}

# Collect results of the successful/failed deployments
SUCCESS_COUNT=0
FAILURE_COUNT=0

for i in "${!VM_ARRAY[@]}"; do
    vm_index=$((i + 1))
    if deploy_to_vm "${VM_ARRAY[$i]}" "$vm_index"; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
    fi
    echo ""
done

# Print summary of operations
echo "========================================"
print_info "Deployment Summary:"
print_info "  Successful: $SUCCESS_COUNT"
if [[ $FAILURE_COUNT -gt 0 ]]; then
    print_error "  Failed: $FAILURE_COUNT"
else
    print_info "  Failed: $FAILURE_COUNT"
fi
echo "========================================"

if [[ $FAILURE_COUNT -gt 0 ]]; then
    exit 1
fi

print_info "Benchmark tool deployed successfully to all load generators!"