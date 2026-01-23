#!/bin/bash

# Utility script to open terminal windows for RabbitMQ benchmark VMs
# Reads IPs from config.txt and opens SSH sessions

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.txt"

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Configuration file '$CONFIG_FILE' not found!"
    echo "Please run 'terraform apply' first to generate the config file."
    exit 1
fi

# Source the config file to get IPs
source "$CONFIG_FILE"

# Function to open a terminal window with SSH connection
open_terminal() {
    local title="$1"
    local ip="$2"
    # Use the specific command requested by the user
    local ssh_cmd="ssh -i ~/.ssh/csb_project_setup benchmarkuser@$ip"
    
    echo "Opening terminal for $title ($ip)..."
    
    # Use ptyxis as the terminal emulator (standard on Ubuntu 24.10+)
    if command -v ptyxis &> /dev/null; then
        ptyxis --title "$title" -- bash -c "$ssh_cmd; exec bash" &
    else
        echo "Error: ptyxis terminal emulator not found."
        echo "Command that would have been run: $ssh_cmd"
        return 1
    fi
}

# 1. Open Load Generator Terminal
if [[ -n "$LOAD_GENERATOR_IP" ]]; then
    open_terminal "Load Generator" "$LOAD_GENERATOR_IP"
else
    echo "Warning: LOAD_GENERATOR_IP not found in config.txt"
fi

# 2. Open Cluster Node Terminals
if [[ -n "$CLUSTER_NODE_IPS" ]]; then
    # Split comma-separated IPs into array
    IFS=',' read -ra NODE_IPS <<< "$CLUSTER_NODE_IPS"
    
    count=1
    for ip in "${NODE_IPS[@]}"; do
        # Trim whitespace just in case
        ip=$(echo "$ip" | xargs)
        if [[ -n "$ip" ]]; then
            open_terminal "Cluster Node $count" "$ip"
            ((count++))
        fi
    done
else
    echo "Warning: CLUSTER_NODE_IPS not found in config.txt"
fi

echo "Done."
