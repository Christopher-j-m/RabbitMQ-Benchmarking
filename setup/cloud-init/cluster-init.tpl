#cloud-config
package_update: true
package_upgrade: true

packages:
  - erlang-nox
  - rabbitmq-server

runcmd:
  # Set the shared Erlang cookie & Nodename env var
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

    # Configuration vars
    SEED_NODE="${cluster_seed_host}"
    CLUSTER_NAME="${cluster_name}"
    RETRIES=60
    SLEEP_SECONDS=5

    echo "[$(date)] Starting cluster formation script..."
    echo "[$(date)] This node: $(hostname -s), Seed node: $SEED_NODE"

    # Wait for RabbitMQ to be fully started
    echo "[$(date)] Waiting for RabbitMQ process to start..."
    for attempt in $(seq 1 30); do
      if sudo rabbitmqctl await_startup >/dev/null 2>&1; then
        echo "[$(date)] RabbitMQ process is running"
        break
      fi
      if [[ $attempt -eq 30 ]]; then
        echo "[$(date)] ERROR: RabbitMQ process failed to start"
        exit 1
      fi
      sleep 3
    done

    # If this is the seed node, set only cluster name
    if [[ "$(hostname -s)" == "$SEED_NODE" ]]; then
      echo "[$(date)] This is the seed node, setting cluster name: $CLUSTER_NAME"
      sudo rabbitmqctl set_cluster_name "$CLUSTER_NAME"
      exit 0
    fi

    # If this is not the seed node, attempt to join the cluster
    echo "[$(date)] Non-seed node - waiting for seed node to be reachable..."
    for attempt in $(seq 1 "$RETRIES"); do
      # Check if seed node is reachable
      if ping -c 1 -W 1 "$SEED_NODE" >/dev/null 2>&1; then
        echo "[$(date)] Seed node $SEED_NODE is reachable"
        
        # Wait a bit to ensure RabbitMQ is ready on seed
        sleep 10
        echo "[$(date)] Attempting to join cluster..."
        break
      fi

      # Max attempts reached
      if [[ $attempt -eq $RETRIES ]]; then
        echo "[$(date)] ERROR: Seed node not reachable after $RETRIES attempts"
        exit 1
      fi

      # Wait before retrying
      echo "[$(date)] Attempt $attempt/$RETRIES - seed node not yet reachable, waiting..."
      sleep "$SLEEP_SECONDS"
    done

    # Join the cluster
    echo "[$(date)] Stopping RabbitMQ app to join cluster..."
    sudo rabbitmqctl stop_app
    
    echo "[$(date)] Joining cluster rabbit@$SEED_NODE..."
    if sudo rabbitmqctl join_cluster "rabbit@$SEED_NODE"; then
      echo "[$(date)] Successfully joined cluster"
    else
      echo "[$(date)] ERROR: Failed to join cluster"
      sudo rabbitmqctl start_app
      exit 1
    fi
    
    echo "[$(date)] Starting RabbitMQ again..."
    sudo rabbitmqctl start_app
    SCRIPT_EOF

  # Enable management plugin & restart (or rather initial start) RabbitMQ
  - 'rabbitmq-plugins enable rabbitmq_management'
  - 'systemctl restart rabbitmq-server'
  
  # Wait for RabbitMQ to start before running the cluster script above
  - |
    echo "[$(date)] Waiting for RabbitMQ to be ready after restart..."
    for i in {1..30}; do
      if rabbitmqctl await_startup >/dev/null 2>&1; then
        echo "[$(date)] RabbitMQ is ready"
        break
      fi
      if [[ $i -eq 30 ]]; then
        echo "[$(date)] ERROR: RabbitMQ did not start in time"
        exit 1
      fi
      sleep 3
    done
  
  # Debug logs: /var/log/cloud-init-output.log
  - chmod +x /usr/local/bin/rabbitmq-cluster.sh
  - /usr/local/bin/rabbitmq-cluster.sh
  
  # Create admin user for remote access to management UI
  # TODO: Paramerterize admin credentials inside of variables.tf
  - 'rabbitmqctl add_user admin password?123'
  - 'rabbitmqctl set_user_tags admin administrator'
  - 'rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"'