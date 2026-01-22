# RabbitMQ Cluster Benchmarking CLI

> Part of the Cloud Service Benchmarking module at the Technische Universität Berlin during the Winter Term 25/26.

![Project Flow Diagram](assets/project_flow_dg.svg)

## Table of Contents

* [Requirements](#requirements)
* [Setup Benchmark Environment](#setup-benchmark-environment)
* [Benchmark Execution](#benchmark-execution)
* [Adding New Experiments](#adding-new-experiments)

---

## Requirements

The project has been tested on **Ubuntu 22.04** and **24.04**.

| Component | Requirement |
| --- | --- |
| **Setup** | [Terraform](https://developer.hashicorp.com/terraform/install) (1.14+)<br>[Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest) |
| **Load Generator** | [Go](https://go.dev/dl/) (1.25+) |
| **Cluster Nodes** | [RabbitMQ](https://www.rabbitmq.com/docs/download) (3.13+) |

---

## Setup Benchmark Environment

This repository contains an exemplary Terraform setup to provision a benchmark environment on Microsoft Azure. It automates dependency installation, deploys RabbitMQ via `cloud-init` and provides utility scripts to streamline CLI deployment and result collection.

While custom environments (other cloud providers or local machines) are expected to be compatible with the benchmark CLI, it is recommended to stick to the configuration of the provided Terraform setup as closely as possible. In this case, you can skip to the [Benchmark Execution](#benchmark-execution) section.

### Terraform

#### 1. Configuration

Configure the benchmark environment in `variables.tf` before provisioning.

```bash
cd setup/terraform && nano variables.tf

```

#### 2. Provisioning

```bash
terraform init
terraform apply

```

#### 3. Verify RabbitMQ Cluster Status

After provisioning, `cloud-init` installs dependencies, the RabbitMQ Management Plugin and automatically forms the cluster. Before proceeding, verify that the cluster has formed successfully via the [Management Plugin UI](https://www.rabbitmq.com/docs/management#usage-ui) or by running the following on any cluster node:

```bash
rabbitmqctl cluster_status

```

### Automation Scripts

The Terraform setup generates a `config.txt` file in the `setup/utility` directory. This file contains environment metadata used by the automation scripts. These scripts allow you to perform repetitive tasks, such as building and deploying the CLI to the load generator via SSH, without manually passing any parameters. Any use of these scripts is highly optional.

---

## Benchmark Execution

### Build Benchmark CLI

You can build and copy the CLI using the `deploy_benchmark.sh` utility script if you followed the [Setup Benchmark Environment](#setup-benchmark-environment) step, or manually via:

```bash
cd benchmark && go build -o rmq-benchmark .

```

To view all available configuration parameters and default values:

```bash
./rmq-benchmark -h

```

### Available Experiments

The CLI currently offers three independent experiments focused on measuring end-to-end latency and throughput. Select an experiment using the `--experiment <name>` parameter. 

The examples below represent configurations used to generate the results in the `/measurements` directory after following [Setup Benchmark Environment](#setup-benchmark-environment) and deploying 3, 6, and 9-node RabbitMQ clusters respectively.

---

#### 1. Throughput (*linear-capacity*)
Measures the maximum throughput of the RabbitMQ cluster.

**Example (all cluster sizes):**
```bash
./rmq-benchmark --mgmt-url="http://<cluster-node-ip>:15672" --rmq-user="your-admin-user" --rmq-password="your-password" --experiment=linear-capacity --publishers=160 --queue-length=1000 --warmup=0 --duration=3600 --queue-count=4

```

---

#### 2. Baseline Latency (*ideal-latency*)
Measures the latency cost imposed by [Raft consensus](https://www.rabbitmq.com/docs/quorum-queues#behaviour) under ideal conditions, using a fixed 1-publisher/1-consumer setup connected directly to the queue leader node.

| Cluster Size | Parameters |
| :--- | :--- |
| **3 Nodes** | ```./rmq-benchmark --mgmt-url="http://<cluster-node-ip>:15672" --rmq-user="your-admin-user" --rmq-password="your-password" --experiment=ideal-latency --warmup=0 --duration=3600 --quorum-size=3``` |
| **6 Nodes** | ```./rmq-benchmark --mgmt-url="http://<cluster-node-ip>:15672" --rmq-user="your-admin-user" --rmq-password="your-password" --experiment=ideal-latency --warmup=0 --duration=3600 --quorum-size=6``` |
| **9 Nodes** | `./rmq-benchmark --mgmt-url="http://<cluster-node-ip>:15672" --rmq-user="your-admin-user" --rmq-password="your-password" --experiment=ideal-latency --warmup=0 --duration=3600 --quorum-size=9` |

---

#### 3. Contention Latency (*stress-latency*)
Measures end-to-end latency in RabbitMQ Quorum Queues under heavy load. This experiment re-uses the `ideal-latency` setup for measurement and the `linear-capacity` approach for parallel stress generation.

| Cluster Size | Parameters |
| :--- | :--- |
| **3 Nodes** | `./rmq-benchmark --mgmt-url="http://<cluster-node-ip>:15672" --rmq-user="your-admin-user" --rmq-password="your-password" --experiment=stress-latency --publishers=10 --queue-length=1000 --warmup=0 --duration=3600 --queue-count=4 --quorum-size=3` |
| **6 Nodes** | `./rmq-benchmark --mgmt-url="http://<cluster-node-ip>:15672" --rmq-user="your-admin-user" --rmq-password="your-password" --experiment=stress-latency --publishers=10 --queue-length=1000 --warmup=0 --duration=3600 --queue-count=4 --quorum-size=6` |
| **9 Nodes** | `./rmq-benchmark --mgmt-url="http://<cluster-node-ip>:15672" --rmq-user="your-admin-user" --rmq-password="your-password" --experiment=stress-latency --publishers=10 --queue-length=1000 --warmup=0 --duration=3600 --queue-count=4 --quorum-size=9` |

> **Note:** Make sure that `--mgmt-url` points to a specific node within the RabbitMQ cluster that has the [RabbitMQ Management Plugin](https://www.rabbitmq.com/docs/management) installed.

---

## Adding New Experiments

To extend the benchmark suite with a new benchmark experiment:

1. Implement the experiment interface defined in `/experiments/interface.go`.
2. Register the new experiment in `/experiments/registry.go`.
3. Rebuild the CLI to automatically make the new experiment available via the `-experiment` parameter.
