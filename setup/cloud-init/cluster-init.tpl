#cloud-config
package_update: true
package_upgrade: true

packages:
  - curl
  - iotop
  - ifstat
  - xfsprogs
  - gnupg
  - apt-transport-https

runcmd:
  # Mount Premium SSD v2 data disk
  - |
    #!/bin/bash
    set -e
    echo "Setting up Premium SSD v2 data disk..."
    
    # Premium SSD v2 on v6-series VMs uses NVMe
    # nvme0n1 = OS disk, nvme0n2 = data disk
    # Source: https://learn.microsoft.com/de-de/azure/virtual-machines/nvme-overview#scsi-to-nvme
    DEVICE="/dev/nvme0n2"
    
    # Wait up to 60 seconds for the NVMe data disk to appear
    for i in $(seq 1 60); do
      if [ -b "$DEVICE" ]; then
        echo "Found NVMe data disk at $DEVICE"
        break
      fi
      
      if [ "$i" -eq 60 ]; then
        echo "ERROR: Data disk $DEVICE not found after 60 seconds"
        echo "Available devices:"
        lsblk
        exit 1
      fi
      echo "Waiting for data disk... ($i/60)"
      sleep 1
    done
    
    echo "Data disk device: $DEVICE"
    
    # Format disk if not already formatted with XFS
    if ! blkid "$DEVICE" | grep -q xfs; then
      echo "Formatting $DEVICE with XFS..."
      mkfs.xfs -f "$DEVICE"
    else
      echo "Disk already formatted with XFS"
    fi
    
    # Get the UUID for fstab entry
    UUID=$(blkid -s UUID -o value "$DEVICE")
    echo "Disk UUID: $UUID"
    
    if [ -z "$UUID" ]; then
      echo "ERROR: Could not get UUID for $DEVICE"
      exit 1
    fi
    
    # Create mount
    mkdir -p /var/lib/rabbitmq
    
    # Add fstab entry if not already present
    if ! grep -q "$UUID" /etc/fstab; then
      echo "UUID=$UUID /var/lib/rabbitmq xfs defaults,nofail 0 2" >> /etc/fstab
      echo "Added fstab entry for data disk"
    fi
    
    # Mount the disk
    mount /var/lib/rabbitmq
    echo "Data disk mounted at /var/lib/rabbitmq"
    df -h /var/lib/rabbitmq

  # Install RabbitMQ 4.2.2 and Erlang
  - |
    echo "Starting RabbitMQ 4.2.2 installation..."

    # Import the official Team RabbitMQ signing key
    curl -1sLf "https://keys.openpgp.org/vks/v1/by-fingerprint/0A9AF2115F4687BD29803A206B73A36E6026DFCA" | gpg --dearmor | tee /usr/share/keyrings/com.rabbitmq.team.gpg > /dev/null

    # Add apt repositories maintained by Team RabbitMQ
    cat > /etc/apt/sources.list.d/rabbitmq.list <<EOF
    deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb1.rabbitmq.com/rabbitmq-erlang/ubuntu/noble noble main
    deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb2.rabbitmq.com/rabbitmq-erlang/ubuntu/noble noble main
    deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb1.rabbitmq.com/rabbitmq-server/ubuntu/noble noble main
    deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb2.rabbitmq.com/rabbitmq-server/ubuntu/noble noble main
    EOF

    # Update package index
    apt-get update -y

    # Install Erlang packages
    apt-get install -y erlang-base \
                        erlang-asn1 erlang-crypto erlang-eldap erlang-ftp erlang-inets \
                        erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key \
                        erlang-runtime-tools erlang-snmp erlang-ssl \
                        erlang-syntax-tools erlang-tftp erlang-tools erlang-xmerl

    # Install RabbitMQ
    apt-get install -y rabbitmq-server=4.2.2-1

    # Pin the rmq version to prevent automatic upgrades
    apt-mark hold rabbitmq-server

    echo "RabbitMQ 4.2.2 installed"

  # Set the shared Erlang cookie & Nodename env var BEFORE starting RabbitMQ
  - |
    HOSTNAME=$(hostname -s)
    
    echo "${erlang_cookie}" > /var/lib/rabbitmq/.erlang.cookie
    chown rabbitmq:rabbitmq /var/lib/rabbitmq/.erlang.cookie
    chmod 400 /var/lib/rabbitmq/.erlang.cookie
    cat <<EOF >/etc/rabbitmq/rabbitmq-env.conf
    NODENAME=rabbit@$${HOSTNAME}
    EOF

  # Create script to handle cluster formation
  - |
    cat <<'SCRIPT_EOF' >/usr/local/bin/rabbitmq-cluster.sh
    #!/usr/bin/env bash
    set -euo pipefail

    SEED_NODE="${cluster_seed_host}"
    CLUSTER_NAME="${cluster_name}"

    echo "Starting RabbitMQ cluster formation..."

    # Wait for local RabbitMQ
    echo "Waiting for local RabbitMQ..."
    timeout 180 bash -c 'until sudo rabbitmqctl await_startup 2>/dev/null; do sleep 3; done'
    echo "Local RabbitMQ is running"

    # If this is the seed node, set the cluster name
    if [[ "$(hostname -s)" == "$SEED_NODE" ]]; then
      sudo rabbitmqctl set_cluster_name "$CLUSTER_NAME"
      echo "Seed node setup complete"
      exit 0
    fi

    # If this is not the seed node, wait for seed node's RabbitMQ to be ready (up to 10 min to avoid race-conditions during terraform)
    echo "Waiting for seed node rabbit@$SEED_NODE..."
    timeout 600 bash -c "until sudo rabbitmqctl -n rabbit@$SEED_NODE ping 2>/dev/null; do sleep 5; done"
    echo "Seed node is ready"

    # Join cluster
    for i in {1..30}; do
      echo "Join attempt $i/30..."
      
      # Check if already in cluster
      if sudo rabbitmqctl cluster_status 2>/dev/null | grep -q "rabbit@$SEED_NODE"; then
        echo "Already in cluster"; exit 0
      fi

      sudo rabbitmqctl stop_app
      [[ $i -gt 1 ]] && sudo rabbitmqctl reset 2>/dev/null || true
      
      if sudo rabbitmqctl join_cluster "rabbit@$SEED_NODE" && sudo rabbitmqctl start_app; then
        echo "Successfully joined cluster"; exit 0
      fi
      
      sudo rabbitmqctl start_app 2>/dev/null || true
      sleep 10
    done

    echo "ERROR: Failed to join cluster after 30 attempts"
    exit 1
    SCRIPT_EOF

  # Enable management plugin for monitoring
  - 'rabbitmq-plugins enable rabbitmq_management'
  
  # Restart RabbitMQ to load the management plugin
  - |
    echo "Restarting RabbitMQ to load plugins..."
    df -h /var/lib/rabbitmq
    systemctl restart rabbitmq-server
  
  # Wait for RabbitMQ to start before running the cluster script
  - |
    #!/bin/bash
    echo "Waiting for RabbitMQ to be ready after restart..."
    for i in $(seq 1 30); do
      if rabbitmqctl await_startup >/dev/null 2>&1; then
        echo "RabbitMQ is ready"
        break
      fi
      if [ "$i" -eq 30 ]; then
        echo "ERROR: RabbitMQ did not start in time"
        exit 1
      fi
      sleep 3
    done
  
  # Debug logs: /var/log/cloud-init-output.log
  - chmod +x /usr/local/bin/rabbitmq-cluster.sh
  - /usr/local/bin/rabbitmq-cluster.sh
  
  # Create admin user for remote access to management UI
  - 'rabbitmqctl add_user ${rabbitmq_username} ${rabbitmq_password}'
  - 'rabbitmqctl set_user_tags ${rabbitmq_username} administrator'
  - 'rabbitmqctl set_permissions -p / ${rabbitmq_username} ".*" ".*" ".*"'