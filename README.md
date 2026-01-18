# RabbitMQ-Cluster Benchmarking CLI

> Part of the Cloud Service Benchmarking module at the Technische Universität Berlin during the Winter Term 25/26.

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
| **Setup** | [Terraform](https://developer.hashicorp.com/terraform/install) (1.14+)<br>[Azure CLI](https://www.google.com/search?q=https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) |
| **Load Generator** | [Go](https://go.dev/dl/) (1.25+) |
| **Cluster Nodes** | [RabbitMQ](https://www.rabbitmq.com/docs/download) (v3.13+) |

---

## Setup Benchmark Environment

This repository contains an exemplary Terraform setup to provision a benchmark environment on Microsoft Azure. It automates dependency installation, deploys RabbitMQ via `cloud-init` and provides utility scripts to streamline CLI deployment and result collection.

While custom environments (other cloud providers or local machines) are expected to be compatible with the benchmark CLI, it is recommended to stick to the configuration of the provided Terraform setup as closely as possible. In this case, you can skip to the [benchmark execution](#benchmark-execution).

### Terraform (Microsoft Azure)

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

You can build and copy the CLI using the `deploy_benchmark.sh` utility script, or manually via:

```bash
cd benchmark && go build -o rmq-benchmark .

```

To view all available configuration parameters and default values:

```bash
./rmq-benchmark -h

```

### Available Experiments

The CLI currently offers three independent experiments focused on measuring end-to-end latency and throughput. Select an experiment using the `-experiment <name>` parameter.

#### 1. Throughput (*linear-capacity*)

Measures the maximum throughput of the RabbitMQ cluster. **Example:**

```bash
./rmq-benchmark --mgmt-url="http://10.0.1.7:15672" --rmq-user="admin" --rmq-password="your-password" --experiment=linear-capacity --publishers=160 --queue-length=1000 --warmup=120 --duration=1800 --queue-count=4

```

#### 2. Baseline Latency (*ideal-latency*)

Measures the latency cost imposed by the [Raft consensus](https://www.rabbitmq.com/docs/quorum-queues#behaviour) under ideal conditions, using a fixed 1-publisher/1-consumer setup connected directly to the queue leader node. **Example:**

```bash
./rmq-benchmark --mgmt-url="http://10.0.1.7:15672" --rmq-user="admin" --rmq-password="your-password" --experiment=ideal-latency --warmup=120 --duration=1800

```

#### 3. Contention Latency (stress-latency)

Measures end-to-end latency in RabbitMQ Quorum Queues under heavy load. This experiment re-uses the approach of the `ideal-latency` for measuring the latency and the `linear-capacity` experiment for parallel stress generation on the cluster. **Example:**

```bash
./rmq-benchmark --mgmt-url="http://10.0.1.7:15672" --rmq-user="admin" --rmq-password="your-password" --experiment=stress-latency --publishers=160 --queue-length=1000 --warmup=120 --duration=1800 --queue-count=4

```

---

## Adding New Experiments

To extend the benchmark suite with a new independent benchmark experiment:

1. Implement the experiment interface defined in `/experiments/interface.go`.
2. Register the new experiment in `/experiments/registry.go`.
3. Rebuild the CLI to automatically make the new experiment available via the `-experiment` parameter.
