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
  - 'rabbitmq-plugins enable rabbitmq_management'
  - 'rabbitmq-plugins enable rabbitmq_prometheus'
  - 'systemctl restart rabbitmq-server'
  
  # Wait for RabbitMQ to be ready
  - |
    echo "[$(date)] Waiting for RabbitMQ to be ready..."
    for i in {1..30}; do
      if rabbitmqctl await_startup >/dev/null 2>&1; then
        echo "[$(date)] RabbitMQ is ready"
        break
      fi
      sleep 2
    done
  
  # Create admin user
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
  
  # Download and import RabbitMQ Grafana dashboards
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
      echo "[$(date)] Failed to download dashboard 10991"
    fi
  
  # Source: https://grafana.com/grafana/dashboards/6566-rabbitmq-perftest/
  - |
    echo "[$(date)] Importing RabbitMQ Perftest dashboard (6566)..."
    curl -s https://grafana.com/api/dashboards/6566/revisions/1/download -o /tmp/rabbitmq-dashboard-6566.json
    if [ -f /tmp/rabbitmq-dashboard-6566.json ]; then
      jq -n --slurpfile dashboard /tmp/rabbitmq-dashboard-6566.json \
        '{dashboard: $dashboard[0], overwrite: true, inputs: [{name: "DS_PROMETHEUS", type: "datasource", pluginId: "prometheus", value: "Prometheus"}]}' \
        > /tmp/dashboard-import-6566.json
      
      curl -X POST -H "Content-Type: application/json" \
        -u admin:admin \
        -d @/tmp/dashboard-import-6566.json \
        http://localhost:3000/api/dashboards/import || echo "[$(date)] Failed to import dashboard 6566"
    else
      echo "[$(date)] Failed to download dashboard 6566"
    fi
  
  - 'echo "[$(date)] Monitoring setup complete"'