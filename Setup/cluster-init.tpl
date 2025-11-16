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
    cat <<'EOF' >/usr/local/bin/rabbitmq-cluster.sh
    #!/usr/bin/env bash
    set -euo pipefail

    # Configuration vars
    SEED_NODE="rabbit-cluster-node-1"
    CLUSTER_NAME="rmq-benchmark-cluster"
    RETRIES=12
    SLEEP_SECONDS=5

    # If this is the seed node, set only cluster name
    if [[ "$(hostname -s)" == "$SEED_NODE" ]]; then
      sudo rabbitmqctl set_cluster_name "$CLUSTER_NAME"
      exit 0
    fi

    # If this is not the seed node, attempt to join the cluster
    for attempt in $(seq 1 "$RETRIES"); do

      # Check if seed node is reachable
      if ping -c 1 -W 1 "$SEED_NODE" >/dev/null 2>&1; then
        break
      fi

      # Max attempts reached
      if [[ $attempt -eq $RETRIES ]]; then
        exit 1
      fi

      # Wait before retrying
      sleep "$SLEEP_SECONDS"
    done

    # Stop rmq in order to join cluster, and re-start rmq again
    sudo rabbitmqctl stop_app
    sudo rabbitmqctl join_cluster "rabbit@$SEED_NODE"
    sudo rabbitmqctl start_app
    EOF

  # Enable management plugin, restart (or rather initial start) RabbitMQ and run cluster script
  - 'rabbitmq-plugins enable rabbitmq_management'
  - 'systemctl restart rabbitmq-server'
  - chmod +x /usr/local/bin/rabbitmq-cluster.sh
  - /usr/local/bin/rabbitmq-cluster.sh
  
  # Create admin user for remote access to management UI
  - 'rabbitmqctl add_user admin <admin_password>'
  - 'rabbitmqctl set_user_tags admin administrator'
  - 'rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"'