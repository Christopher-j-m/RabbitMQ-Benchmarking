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
  }
}

provider "azurerm" {
  features {}
}

# Setup network resources (VNet, Subnet, Firewall)
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_virtual_network" "vnet" {
  name                = var.virtual_network_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "subnet" {
  name                 = "rabbitmq-subnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

resource "azurerm_network_security_group" "nsg" {
  name                = "rabbitmq-nsg"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

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

resource "azurerm_subnet_network_security_group_association" "nsg_assoc" {
  subnet_id                 = azurerm_subnet.subnet.id
  network_security_group_id = azurerm_network_security_group.nsg.id
}

# Generate a random Erlang cookie for cluster authentication & define cloud-init templates
resource "random_string" "erlang_cookie" {
  length  = 32
  special = false
}

data "template_file" "cluster_init" {
  template = file(var.cluster_nodes.cloud_init_file_path)
  vars = {
    erlang_cookie     = random_string.erlang_cookie.result
    cluster_seed_host = format("%s-%02d", var.cluster_nodes.name_prefix, 1)
    cluster_name      = var.cluster_nodes.cluster_name
  }
}

data "template_file" "single_node_init" {
  template = file(var.single_node.cloud_init_file_path)
  vars = {
    erlang_cookie = random_string.erlang_cookie.result
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
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "cluster_nic" {
  count               = var.cluster_nodes.count
  name                = format("%s-nic-%02d", var.cluster_nodes.name_prefix, count.index + 1)
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.cluster_pip[count.index].id
  }
}

resource "azurerm_linux_virtual_machine" "cluster_vm" {
  count                 = var.cluster_nodes.count
  name                  = format("%s-%02d", var.cluster_nodes.name_prefix, count.index + 1)
  resource_group_name   = azurerm_resource_group.rg.name
  location              = azurerm_resource_group.rg.location
  size                  = var.cluster_nodes.size
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
  }

  source_image_reference {
    publisher = var.cluster_nodes.source_image.publisher
    offer     = var.cluster_nodes.source_image.offer
    sku       = var.cluster_nodes.source_image.sku
    version   = var.cluster_nodes.source_image.version
  }
}

# Single VM for the single RabbitMQ node setup
resource "azurerm_public_ip" "single_pip" {
  count               = var.single_node.enabled ? 1 : 0
  name                = "${var.single_node.name}-pip"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "single_nic" {
  count               = var.single_node.enabled ? 1 : 0
  name                = "${var.single_node.name}-nic"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.single_pip[count.index].id
  }
}

resource "azurerm_linux_virtual_machine" "single_vm" {
  count                 = var.single_node.enabled ? 1 : 0
  name                  = var.single_node.name
  resource_group_name   = azurerm_resource_group.rg.name
  location              = azurerm_resource_group.rg.location
  size                  = var.single_node.size
  admin_username        = var.single_node.admin_username
  custom_data           = base64encode(data.template_file.single_node_init.rendered)
  network_interface_ids = [azurerm_network_interface.single_nic[count.index].id]

  admin_ssh_key {
    username   = var.single_node.admin_username
    public_key = file(var.single_node.admin_ssh_key_path)
  }

  os_disk {
    caching              = var.single_node.os_disk.caching
    storage_account_type = var.single_node.os_disk.storage_account_type
  }

  source_image_reference {
    publisher = var.single_node.source_image.publisher
    offer     = var.single_node.source_image.offer
    sku       = var.single_node.source_image.sku
    version   = var.single_node.source_image.version
  }
}

# Load Generator VMs
resource "azurerm_public_ip" "loadgen_pip" {
  count               = var.load_generators.count
  name                = format("%s-pip-%02d", var.load_generators.name_prefix, count.index + 1)
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "loadgen_nic" {
  count               = var.load_generators.count
  name                = format("%s-nic-%02d", var.load_generators.name_prefix, count.index + 1)
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.loadgen_pip[count.index].id
  }
}

resource "azurerm_linux_virtual_machine" "loadgen_vm" {
  count                 = var.load_generators.count
  name                  = format("%s-%02d", var.load_generators.name_prefix, count.index + 1)
  resource_group_name   = azurerm_resource_group.rg.name
  location              = azurerm_resource_group.rg.location
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
  }

  source_image_reference {
    publisher = var.load_generators.source_image.publisher
    offer     = var.load_generators.source_image.offer
    sku       = var.load_generators.source_image.sku
    version   = var.load_generators.source_image.version
  }
}