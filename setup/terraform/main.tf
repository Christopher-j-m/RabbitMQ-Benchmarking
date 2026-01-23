terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~>3.1"
    }
    null = {
      source  = "hashicorp/null"
      version = "~>3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Setup network resources (VNet, Subnet, Firewall)
resource "azurerm_resource_group" "rabbitmq_benchmark_rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_virtual_network" "rabbitmq_vnet" {
  name                = var.virtual_network_name
  resource_group_name = azurerm_resource_group.rabbitmq_benchmark_rg.name
  location            = azurerm_resource_group.rabbitmq_benchmark_rg.location
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "rabbitmq_subnet" {
  name                 = "rabbitmq-subnet"
  resource_group_name  = azurerm_resource_group.rabbitmq_benchmark_rg.name
  virtual_network_name = azurerm_virtual_network.rabbitmq_vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

resource "azurerm_network_security_group" "rabbitmq_nsg" {
  name                = "rabbitmq-nsg"
  location            = azurerm_resource_group.rabbitmq_benchmark_rg.location
  resource_group_name = azurerm_resource_group.rabbitmq_benchmark_rg.name

  security_rule {
    name                       = "AllowSSH"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  security_rule {
    name                       = "AllowMgmtUI"
    priority                   = 120
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "15672"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  security_rule {
    name                       = "AllowAMQP"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5672"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
  }
  security_rule {
    name                       = "AllowEPMD"
    priority                   = 130
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "4369"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
  }
  security_rule {
    name                       = "AllowCluster"
    priority                   = 140
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "25672"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
  }
}

resource "azurerm_subnet_network_security_group_association" "rabbitmq_nsg_assoc" {
  subnet_id                 = azurerm_subnet.rabbitmq_subnet.id
  network_security_group_id = azurerm_network_security_group.rabbitmq_nsg.id
}

# Generate a random Erlang cookie for cluster authentication & define cloud-init templates
resource "random_string" "erlang_cookie" {
  length  = 32
  special = false
}

resource "random_password" "rabbitmq_password" {
  count            = var.rabbitmq_admin_password == null ? 1 : 0
  length           = 16
  special          = false
}

locals {
  rabbitmq_admin_password = var.rabbitmq_admin_password != null ? var.rabbitmq_admin_password : random_password.rabbitmq_password[0].result
}

data "template_file" "cluster_init" {
  template = file(var.cluster_nodes.cloud_init_file_path)
  vars = {
    erlang_cookie     = random_string.erlang_cookie.result
    cluster_seed_host = format("%s-%02d", var.cluster_nodes.name_prefix, 1)
    cluster_name      = var.cluster_nodes.cluster_name
    rabbitmq_password = local.rabbitmq_admin_password
    rabbitmq_username = var.rabbitmq_admin_username
  }
}

data "template_file" "loadgen_init" {
  count    = var.load_generators.cloud_init_file_path != null ? 1 : 0
  template = file(var.load_generators.cloud_init_file_path)
  vars = {
    erlang_cookie = random_string.erlang_cookie.result
  }
}

# Create the VMs for the RabbitMQ cluster
resource "azurerm_public_ip" "cluster_pip" {
  count               = var.cluster_nodes.count
  name                = format("%s-pip-%02d", var.cluster_nodes.name_prefix, count.index + 1)
  location            = azurerm_resource_group.rabbitmq_benchmark_rg.location
  resource_group_name = azurerm_resource_group.rabbitmq_benchmark_rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "cluster_nic" {
  count               = var.cluster_nodes.count
  name                = format("%s-nic-%02d", var.cluster_nodes.name_prefix, count.index + 1)
  location            = azurerm_resource_group.rabbitmq_benchmark_rg.location
  resource_group_name = azurerm_resource_group.rabbitmq_benchmark_rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.rabbitmq_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.cluster_pip[count.index].id
  }
}

resource "azurerm_linux_virtual_machine" "cluster_vm" {
  count                 = var.cluster_nodes.count
  name                  = format("%s-%02d", var.cluster_nodes.name_prefix, count.index + 1)
  resource_group_name   = azurerm_resource_group.rabbitmq_benchmark_rg.name
  location              = azurerm_resource_group.rabbitmq_benchmark_rg.location
  size                  = var.cluster_nodes.size
  zone                  = var.cluster_nodes.zone
  admin_username        = var.cluster_nodes.admin_username
  custom_data           = base64encode(data.template_file.cluster_init.rendered)
  network_interface_ids = [azurerm_network_interface.cluster_nic[count.index].id]

  admin_ssh_key {
    username   = var.cluster_nodes.admin_username
    public_key = file(var.cluster_nodes.admin_ssh_key_path)
  }

  os_disk {
    caching              = var.cluster_nodes.os_disk.caching
    storage_account_type = var.cluster_nodes.os_disk.storage_account_type
    disk_size_gb         = var.cluster_nodes.os_disk.disk_size_gb
  }

  source_image_reference {
    publisher = var.cluster_nodes.source_image.publisher
    offer     = var.cluster_nodes.source_image.offer
    sku       = var.cluster_nodes.source_image.sku
    version   = var.cluster_nodes.source_image.version
  }
}

resource "azurerm_managed_disk" "cluster_data_disk" {
  count                = var.cluster_nodes.count
  name                 = format("%s-datadisk-%02d", var.cluster_nodes.name_prefix, count.index + 1)
  location             = azurerm_resource_group.rabbitmq_benchmark_rg.location
  resource_group_name  = azurerm_resource_group.rabbitmq_benchmark_rg.name
  storage_account_type = "PremiumV2_LRS"
  create_option        = "Empty"
  disk_size_gb         = var.cluster_nodes.data_disk.size_gb
  disk_iops_read_write = var.cluster_nodes.data_disk.iops_read_write
  disk_mbps_read_write = var.cluster_nodes.data_disk.mbps_read_write
  zone                 = var.cluster_nodes.zone
}

resource "azurerm_virtual_machine_data_disk_attachment" "cluster_data_disk_attach" {
  count              = var.cluster_nodes.count
  managed_disk_id    = azurerm_managed_disk.cluster_data_disk[count.index].id
  virtual_machine_id = azurerm_linux_virtual_machine.cluster_vm[count.index].id
  lun                = 10
  caching            = "None"
}

# Load Generator VMs
resource "azurerm_public_ip" "loadgen_pip" {
  count               = var.load_generators.count
  name                = format("%s-pip-%02d", var.load_generators.name_prefix, count.index + 1)
  location            = azurerm_resource_group.rabbitmq_benchmark_rg.location
  resource_group_name = azurerm_resource_group.rabbitmq_benchmark_rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "loadgen_nic" {
  count               = var.load_generators.count
  name                = format("%s-nic-%02d", var.load_generators.name_prefix, count.index + 1)
  location            = azurerm_resource_group.rabbitmq_benchmark_rg.location
  resource_group_name = azurerm_resource_group.rabbitmq_benchmark_rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.rabbitmq_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.loadgen_pip[count.index].id
  }
}

resource "azurerm_linux_virtual_machine" "loadgen_vm" {
  count                 = var.load_generators.count
  name                  = format("%s-%02d", var.load_generators.name_prefix, count.index + 1)
  resource_group_name   = azurerm_resource_group.rabbitmq_benchmark_rg.name
  location              = azurerm_resource_group.rabbitmq_benchmark_rg.location
  size                  = var.load_generators.size
  admin_username        = var.load_generators.admin_username
  custom_data           = var.load_generators.cloud_init_file_path != null ? base64encode(data.template_file.loadgen_init[0].rendered) : null
  network_interface_ids = [azurerm_network_interface.loadgen_nic[count.index].id]

  admin_ssh_key {
    username   = var.load_generators.admin_username
    public_key = file(var.load_generators.admin_ssh_key_path)
  }

  os_disk {
    caching              = var.load_generators.os_disk.caching
    storage_account_type = var.load_generators.os_disk.storage_account_type
    disk_size_gb         = var.load_generators.os_disk.disk_size_gb
  }

  source_image_reference {
    publisher = var.load_generators.source_image.publisher
    offer     = var.load_generators.source_image.offer
    sku       = var.load_generators.source_image.sku
    version   = var.load_generators.source_image.version
  }
}

# Save VM info to a separate config file for the utility scripts
resource "null_resource" "generate_utility_config" {
  triggers = {
    all_vm_names = join(",", concat(
      azurerm_linux_virtual_machine.cluster_vm[*].name,
      azurerm_linux_virtual_machine.loadgen_vm[*].name
    ))
    load_generator_ips = join(",", azurerm_public_ip.loadgen_pip[*].ip_address)
    resource_group     = azurerm_resource_group.rabbitmq_benchmark_rg.name
  }

  provisioner "local-exec" {
    command = <<-EOT
      cat > ../utility/config.txt <<-CONFIG
# Azure Benchmark Environment Metadata
# Auto-generated by Terraform: re-run 'terraform apply' to regenerate

# ============================================================================
# AZURE
# ============================================================================

# Azure Resource Group
AZURE_RESOURCE_GROUP=${azurerm_resource_group.rabbitmq_benchmark_rg.name}

# ============================================================================
# SSH
# ============================================================================

# SSH Key Path
SSH_KEY_PATH=${replace(var.load_generators.admin_ssh_key_path, ".pub", "")}

# Remote username for SSH connections
REMOTE_USER=${var.load_generators.admin_username}

# ============================================================================
# VMS
# ============================================================================

# All VM Names
ALL_VM_NAMES=${join(",", concat(
    azurerm_linux_virtual_machine.cluster_vm[*].name,
    azurerm_linux_virtual_machine.loadgen_vm[*].name
))}

# Load Generator VM IP
LOAD_GENERATOR_IP=${azurerm_public_ip.loadgen_pip[0].ip_address}

# Cluster Node IPs
CLUSTER_NODE_IPS=${join(",", azurerm_public_ip.cluster_pip[*].ip_address)}

# ============================================================================
# RABBITMQ
# ============================================================================

RABBITMQ_ADMIN_USER=${var.rabbitmq_admin_username}
RABBITMQ_ADMIN_PASSWORD=${local.rabbitmq_admin_password}
EOT
}

depends_on = [
  azurerm_linux_virtual_machine.cluster_vm,
  azurerm_linux_virtual_machine.loadgen_vm
]
}