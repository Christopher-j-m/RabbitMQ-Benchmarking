#cloud-config
package_update: true
package_upgrade: true

packages:
  - erlang-nox
  - rabbitmq-server
  - prometheus
  - grafana
  - curl
  - jq

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

  # Enable management and Prometheus plugins for monitoring
  - 'rabbitmq-plugins enable rabbitmq_management'
  - 'rabbitmq-plugins enable rabbitmq_prometheus'
  
  # Restart RabbitMQ with all plugins enabled
  - 'systemctl restart rabbitmq-server'
  
  # Wait for RabbitMQ to start before running the cluster script
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
  
  # Verify Prometheus plugin is working
  - |
    echo "[$(date)] Verifying RabbitMQ Prometheus endpoint..."
    for i in {1..10}; do
      if curl -s http://localhost:15692/metrics > /dev/null; then
        echo "[$(date)] RabbitMQ Prometheus endpoint is responding"
        break
      fi
      echo "[$(date)] Waiting for Prometheus endpoint... ($i/10)"
      sleep 3
    done
  
  # Configure Prometheus to scrape RabbitMQ metrics
  - |
    cat <<'EOF' >/etc/prometheus/prometheus.yml
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    
    scrape_configs:
      - job_name: 'prometheus'
        static_configs:
          - targets: ['localhost:9090']
      
      - job_name: 'rabbitmq'
        static_configs:
          - targets: ['localhost:15692']
    EOF
  
  # Restart Prometheus
  - 'systemctl restart prometheus'
  - 'systemctl enable prometheus'
  
  # Wait for Prometheus to be ready
  - |
    echo "[$(date)] Waiting for Prometheus to be ready..."
    for i in {1..20}; do
      if curl -s http://localhost:9090/-/ready > /dev/null; then
        echo "[$(date)] Prometheus is ready"
        break
      fi
      echo "[$(date)] Waiting for Prometheus... ($i/20)"
      sleep 2
    done
  
  # Configure Grafana
  - 'systemctl start grafana-server'
  - 'systemctl enable grafana-server'
  
  # Wait for Grafana to start and be ready
  - |
    echo "[$(date)] Waiting for Grafana to be ready..."
    for i in {1..30}; do
      if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
        echo "[$(date)] Grafana is ready"
        break
      fi
      echo "[$(date)] Waiting for Grafana... ($i/30)"
      sleep 2
    done
  
  # Add Prometheus as Grafana datasource
  - |
    echo "[$(date)] Adding Prometheus datasource to Grafana..."
    curl -X POST -H "Content-Type: application/json" \
      -u admin:admin \
      -d '{"name":"Prometheus","type":"prometheus","url":"http://localhost:9090","access":"proxy","isDefault":true}' \
      http://localhost:3000/api/datasources || echo "[$(date)] Failed to add datasource (may already exist)"
  
  # Download and import RabbitMQ Grafana dashboards. Sources:
  # Source: https://grafana.com/grafana/dashboards/10991-rabbitmq-overview/
  - |
    echo "[$(date)] Importing RabbitMQ Overview dashboard (10991)..."
    curl -s https://grafana.com/api/dashboards/10991/revisions/1/download -o /tmp/rabbitmq-dashboard-10991.json
    if [ -f /tmp/rabbitmq-dashboard-10991.json ]; then
      jq -n --slurpfile dashboard /tmp/rabbitmq-dashboard-10991.json \
        '{dashboard: $dashboard[0], overwrite: true, inputs: [{name: "DS_PROMETHEUS", type: "datasource", pluginId: "prometheus", value: "Prometheus"}]}' \
        > /tmp/dashboard-import-10991.json
      
      curl -X POST -H "Content-Type: application/json" \
        -u admin:admin \
        -d @/tmp/dashboard-import-10991.json \
        http://localhost:3000/api/dashboards/import || echo "[$(date)] Failed to import dashboard 10991"
    else
      echo "[$(date)] Failed to download dashboard"
    fi
  
  - 'echo "[$(date)] Monitoring setup complete"'