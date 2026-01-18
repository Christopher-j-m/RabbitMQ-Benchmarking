# RabbitMQ-Cluster Benchmarking CLI

> Part of the module Cloud Service Benchmarking at the Technische Universität Berlin during the Winter Term 25/26

## Table of Content

## Requirements
Tested on Ubuntu 22.04 & ***TODO: Updated cluster node ubuntu version***.
- **Setup**
    - [Terraform](https://developer.hashicorp.com/terraform/install) (1.14+)
    - Authentication via [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest)
- **Load Generator**
    - [Go](https://go.dev/dl/) (1.25+)
- **Cluster Nodes**
    - [RabbitMQ](https://www.rabbitmq.com/docs/download) (***TODO:Version***)


## Setup Benchmark Environment
This repository contains a exemplary terraform setup to create a benchmark environment on Microsoft Azure, install dependencies and deploy rabbitmq via cloud-init and utility scripts to notably automate deployment of the CLI and the collection of benchmark results. While custom benchmark environments, such as other cloud providers or local machines, are expected to be supported, always make sure to stick to the configuration of the exemplary terraform setup as close as possible. In case you want to skip the Terraform setup, please proceed to ***TODO***

### Terraform (Microsoft Azure)

#### Configuration
Configure the benchmark environment in `variables.tf` before the actual provisioning of the environment.
```
cd setup/terraform && nano variables.tf
```

#### Provisioning
```
terraform init
terraform apply
```
#### Verify RabbitMQ Cluster Status
After the provisioning of the Cloud resources, cloud-init is used to install dependencies and monitoring tools, such as the RabbitMQ Management Plugin and automatically form the RabbitMQ cluster among all cluster nodes. Before proceeding with step ***TODO*** make sure to verify via the RabbitMQ Management Plugin that the cluster formating finished sucessfully. This can either be done via the [Management Plugin UI](https://www.rabbitmq.com/docs/management#usage-ui) or on each node with
```
rabbitmqctl cluster_status
```

### (Optional) Automation Scripts
The terraform setup automatically generates a `config.txt` file with informations about the newly created benchmark environment to `setup/utility`, on which the automation scripts depend on in order to extract environment informations and therefore mostly dont require parameters in order to automate repeating tasks, such as building and deploying the benchmark CLI to the load generator client via SSH. 

## Benchmark Execution
### Build benchmark CLI
The benchmark CLI can be built and copied via the automation script `deploy_benchmark.sh` or manually via 
```
cd benchmark && go build -o rmq-benchmark .
```
All available configuration parameters and their default values can be listed via
```
./rmq-benchmark -h
```

### Available Experiments
The CLI offers currently four independent eperiments that are aimed towards measuring end-to-end latency and throughput, which can be selected via `-experiment <experiment-name>`
#### Throughput (*linear-capacity* ***TODO:Rename?***)
Measures the throughput of a RabbitMQ cluster. Example:
```
./rmq-benchmark --mgmt-url="http://10.0.1.7:15672" --rmq-user="admin" --rmq-password="password" --experiment=linear-capacity --publishers=160 --queue-length=1000 --warmup=120 --duration=1800 --queue-count=4
```

#### Isolated Latency (*ideal-latency*)
Measures the Measures the latency cost imposed by the Raft consensus under ideal conditions with a fixed a fixed 1-publisher/1-consumer setup connected directly to the queue leader node. Example:
```
./rmq-benchmark --mgmt-url="http://10.0.1.7:15672" --rmq-user="admin" --rmq-password="password" --experiment=ideal-latency --warmup=12 --duration=1800
```

#### Latency with parallel load (*stress-latency*)
Measures the end-to-end latency in rmq quorum queues under heavy load conditions by combining the latency measurement approach from *ideal_latency* with heavy stress generation similar to *linear_capacity*. Example:
```
./rmq-benchmark --mgmt-url="http://10.0.1.7:15672" --rmq-user="admin" --rmq-password="iMGpvjPOvRZ75QoZ" --experiment=stress-latency --publishers=160 --queue-length=1000 --warmup=0 --duration=1800 --queue-count=4
```

## Adding new Experiments
All experiments need to implement the experiment interface defined in `/experiments/interface.go` and have to be registered in `/experiments/registry.go`. Afterwards new experiments can be run with the Benchmark CLI