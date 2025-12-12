#!/bin/bash
# Collect benchmark results from load generator VM(s)

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
IFS=',' read -ra VM_ARRAY <<< "$LOAD_GENERATOR_IPS"
for i in "${!VM_ARRAY[@]}"; do
    VM_ARRAY[$i]=$(echo "${VM_ARRAY[$i]}" | xargs)
done

print_info "Starting collection of benchmark results from ${#VM_ARRAY[@]} load generator VM(s)..."
print_info "SSH Key-Path: $SSH_KEY_PATH"
print_info "User: $REMOTE_USER"
print_info "Local Results Directory: $LOCAL_RESULTS_DIR"
echo ""

# Check if rsync is available
if ! command -v rsync &>/dev/null; then
    print_error "rsync is not installed. Please install it first:"
    exit 1
fi

# Create local results directory if it doesn't exist
mkdir -p "$LOCAL_RESULTS_DIR"

REMOTE_RESULTS_DIR="/home/$REMOTE_USER/benchmarking/results"

collect_from_vm() {
    local ip=$1
    local vm_index=$2
    
    print_info "[$vm_index] Collecting results from $REMOTE_USER@$ip..."
    
    # Test SSH connection
    if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" "echo 'SSH connection successful'" &>/dev/null; then
        print_error "[$vm_index] Failed to connect to $ip via SSH"
        return 1
    fi
    
    # Check if remote results directory exists
    if ! ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" "test -d $REMOTE_RESULTS_DIR" &>/dev/null; then
        print_warn "[$vm_index] Results directory not found at $REMOTE_RESULTS_DIR on $ip"
        return 0
    fi
    
    # Count files in remote results directory
    file_count=$(ssh -i "$SSH_KEY_PATH" $SSH_OPTS "$REMOTE_USER@$ip" \
        "find $REMOTE_RESULTS_DIR -type f 2>/dev/null | wc -l")
    
    if [[ "$file_count" -eq 0 ]]; then
        print_warn "[$vm_index] No result files found on $ip"
        return 0
    fi
    
    print_info "[$vm_index] Found $file_count result file(s)"
    
    # Create subdirectory for this VM
    vm_local_dir="$LOCAL_RESULTS_DIR/vm_${ip//./_}"
    mkdir -p "$vm_local_dir"
    
    # Copy results using rsync (preserves timestamps)
    print_info "[$vm_index] Copying results to $vm_local_dir..."
    if rsync -avz -e "ssh -i $SSH_KEY_PATH $SSH_OPTS" \
        "$REMOTE_USER@$ip:$REMOTE_RESULTS_DIR/" "$vm_local_dir/" &>/dev/null; then
        print_info "[$vm_index] Results copied successfully from $ip!"
        return 0
    else
        print_error "[$vm_index] Failed to copy results from $ip using rsync"
        return 1
    fi
}

# Collect results from all VMs
SUCCESS_COUNT=0
FAILURE_COUNT=0
EMPTY_COUNT=0

for i in "${!VM_ARRAY[@]}"; do
    vm_index=$((i + 1))
    if collect_from_vm "${VM_ARRAY[$i]}" "$vm_index"; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
    fi
    echo ""
done

# Print summary
echo "========================================"
print_info "Collection Summary:"
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

# Count total collected files
total_files=$(find "$LOCAL_RESULTS_DIR" -type f 2>/dev/null | wc -l)
print_info "Total files collected: $total_files"
print_info "Results saved to: $LOCAL_RESULTS_DIR"
