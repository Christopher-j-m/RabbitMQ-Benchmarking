#!/bin/bash

# Start or deallocate all Azure VMs specified in config.txt
# Usage: ./manage_vms.sh -start   (to start VMs)
#        ./manage_vms.sh -stop    (to deallocate VMs)

# Exit immediately if a command exits with a non-zero status
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.txt"

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

# Parse operation argument
OPERATION=""
if [[ "$1" == "-start" ]]; then
    OPERATION="start"
elif [[ "$1" == "-stop" ]]; then
    OPERATION="stop"
else
    echo "Usage: $0 -start|-stop"
    echo "  -start    Start all deallocated VMs"
    echo "  -stop     Deallocate all running VMs"
    exit 1
fi

echo "========================================"
if [[ "$OPERATION" == "start" ]]; then
    echo "Azure VM Startup"
else
    echo "Azure VM Shutdown"
fi
echo "========================================"
echo ""

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    print_error "Configuration file '$CONFIG_FILE' not found!"
    print_info "Please run 'terraform apply' first or manually create the config file."
    exit 1
fi

source "$CONFIG_FILE"

# Validate required variables
if [[ -z "$AZURE_RESOURCE_GROUP" ]]; then
    print_error "AZURE_RESOURCE_GROUP is not set in $CONFIG_FILE"
    exit 1
fi

if [[ -z "$ALL_VM_NAMES" ]]; then
    print_error "ALL_VM_NAMES is not set in $CONFIG_FILE"
    exit 1
fi

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    print_error "Azure CLI (az) is not installed!"
    print_info "Install it from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi

# Check if logged in to Azure
if ! az account show &> /dev/null; then
    print_error "Not logged in to Azure!"
    print_info "Run 'az login' first to authenticate."
    exit 1
fi

# Get current Azure subscription
SUBSCRIPTION=$(az account show --query name -o tsv)
print_info "Using Azure subscription: $SUBSCRIPTION"
print_info "Resource Group: $AZURE_RESOURCE_GROUP"
echo ""

# Parse VM names into array & trim whitespaces
# Adapted from: https://unix.stackexchange.com/questions/184863/
IFS=',' read -ra VM_ARRAY <<< "$ALL_VM_NAMES"
for i in "${!VM_ARRAY[@]}"; do
    VM_ARRAY[$i]=$(echo "${VM_ARRAY[$i]}" | xargs)
done

# Filter out empty VM names
FILTERED_VMS=()
for vm in "${VM_ARRAY[@]}"; do
    if [[ -n "$vm" ]]; then
        FILTERED_VMS+=("$vm")
    fi
done

if [[ ${#FILTERED_VMS[@]} -eq 0 ]]; then
    if [[ "$OPERATION" == "start" ]]; then
        print_warn "No VMs found to start!"
    else
        print_warn "No VMs found to deallocate!"
    fi
    exit 0
fi

if [[ "$OPERATION" == "start" ]]; then
    print_info "Found ${#FILTERED_VMS[@]} VM(s) to start"
else
    print_info "Found ${#FILTERED_VMS[@]} VM(s) to deallocate"
fi
echo ""

if [[ "$OPERATION" == "start" ]]; then
    print_step "Starting VMs..."
else
    print_step "Deallocating VMs..."
fi
echo ""

# Function to manage a single VM (start or deallocate)
manage_vm() {
    local vm_name=$1
    local vm_index=$2
    
    if [[ "$OPERATION" == "start" ]]; then
        print_info "[$vm_index/${#FILTERED_VMS[@]}] Starting VM: $vm_name"
    else
        print_info "[$vm_index/${#FILTERED_VMS[@]}] Deallocating VM: $vm_name"
    fi
    
    # Check if VM exists and get its current state
    # Adapted from: https://learn.microsoft.com/en-us/answers/questions/2237557/how-to-get-powerstate-of-vmss
    VM_STATE=$(az vm get-instance-view \
        --name "$vm_name" \
        --resource-group "$AZURE_RESOURCE_GROUP" \
        --query "instanceView.statuses[?starts_with(code, 'PowerState/')].displayStatus" \
        -o tsv 2>/dev/null || echo "Unknown")
    
    if [[ "$VM_STATE" == "Unknown" ]]; then
        print_error "    VM '$vm_name' not found or inaccessible"
        return 1
    fi
    
    print_info "    Current state: $VM_STATE"
    
    # Check if already in desired state
    if [[ "$OPERATION" == "start" ]] && [[ "$VM_STATE" == "VM running" ]]; then
        print_info "    Already running, skipping"
        return 0
    elif [[ "$OPERATION" == "stop" ]] && [[ "$VM_STATE" == "VM deallocated" ]]; then
        print_info "    Already deallocated, skipping"
        return 0
    fi
    
    # Perform the operation
    if [[ "$OPERATION" == "start" ]]; then
        if az vm start \
            --name "$vm_name" \
            --resource-group "$AZURE_RESOURCE_GROUP" \
            --no-wait &> /dev/null; then
            print_info "    Start command sent successfully"
            return 0
        else
            print_error "    Failed to send start command to VM"
            return 1
        fi
    else
        if az vm deallocate \
            --name "$vm_name" \
            --resource-group "$AZURE_RESOURCE_GROUP" \
            --no-wait &> /dev/null; then
            print_info "    Deallocate command sent successfully"
            return 0
        else
            print_error "    Failed to send deallocate command to VM"
            return 1
        fi
    fi
}

# Wait for all operations to complete (or at least in the ongoing process, even if not yet finished)
if [[ "$OPERATION" == "start" ]]; then
    print_step "Waiting for all VMs to be in running state..."
    print_info "This may take a bit..."
else
    print_step "Waiting for all VMs to be in deallocated state..."
    print_info "This may take a bit..."
fi
echo ""

for vm in "${FILTERED_VMS[@]}"; do
    if [[ "$OPERATION" == "start" ]]; then
        az vm wait \
            --name "$vm" \
            --resource-group "$AZURE_RESOURCE_GROUP" \
            --custom "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0] == 'PowerState/running'" \
            --timeout 300 \
            &> /dev/null || print_warn "Timeout waiting for $vm"
    else
        az vm wait \
            --name "$vm" \
            --resource-group "$AZURE_RESOURCE_GROUP" \
            --custom "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0] == 'PowerState/deallocated'" \
            --timeout 180 \
            &> /dev/null || print_warn "Timeout waiting for $vm"
    fi
done

if [[ "$OPERATION" == "start" ]]; then
    print_info "All VMs started successfully."
    echo ""
else
    print_info "All VMs deallocated successfully."
fi
echo ""
