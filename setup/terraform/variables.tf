variable "resource_group_name" {
  description = "Name of the Azure resource group that will host all benchmarking resources."
  type        = string
  default     = "rabbitmq-benchmark-rg"
}

variable "location" {
  description = "The Azure region where the VMs will be created."
  type        = string
  default     = "germanywestcentral"
}

variable "virtual_network_name" {
  description = "Name of the virtual network created for the benchmark environment."
  type        = string
  default     = "rabbitmq-vnet"
}

variable "rabbitmq_admin_username" {
  description = "Username for the RabbitMQ admin user."
  type        = string
  default     = "admin"
}

variable "rabbitmq_admin_password" {
  description = "Password for the RabbitMQ admin user. If not specified, a random password will be generated."
  type        = string
  default     = null
  sensitive   = true
}

variable "cluster_nodes" {
  description = "Configuration for the RabbitMQ cluster VMs."
  type = object({
    count                = number
    name_prefix          = string
    cluster_name         = string
    size                 = string
    zone                 = string
    admin_username       = string
    admin_ssh_key_path   = string
    cloud_init_file_path = string
    os_disk = object({
      storage_account_type = string
      caching              = string
      disk_size_gb         = number
    })
    data_disk = object({
      size_gb            = number
      iops_read_write    = number
      mbps_read_write    = number
    })
    source_image = object({
      publisher = string
      offer     = string
      sku       = string
      version   = string
    })
  })
  default = {
    count                = 3
    name_prefix          = "rabbit-cluster-node"
    cluster_name         = "rmq-benchmark-cluster"
    size                 = "Standard_D2s_v5"
    zone                 = "1"
    admin_username       = "azureuser"
    admin_ssh_key_path   = "~/.ssh/csb_project_setup.pub"
    cloud_init_file_path = "../cloud-init/cluster-init.tpl"
    os_disk = {
      storage_account_type = "Premium_LRS"
      caching              = "ReadWrite"
      disk_size_gb         = 30
    }
    // Premium SSD v2 Data Disks to run rmq on it
    // => Cheaper than higher tier OS-disks with consistent IOPS & without burst
    data_disk = {
      size_gb            = 32
      iops_read_write    = 3000
      mbps_read_write    = 125
    }
    source_image = {
      publisher = "Canonical"
      offer     = "0001-com-ubuntu-server-jammy"
      sku       = "22_04-lts-gen2"
      version   = "latest"
    }
  }
}

variable "load_generators" {
  description = "Configuration for load generator VMs."
  type = object({
    count                = number
    name_prefix          = string
    size                 = string
    admin_username       = string
    admin_ssh_key_path   = string
    cloud_init_file_path = optional(string)
    os_disk = object({
      storage_account_type = string
      caching              = string
      disk_size_gb         = number
    })
    source_image = object({
      publisher = string
      offer     = string
      sku       = string
      version   = string
    })
  })
  default = {
    count                = 1
    name_prefix          = "rabbit-loadgen-node"
    size                 = "Standard_F8s_v2"
    admin_username       = "azureuser"
    admin_ssh_key_path   = "~/.ssh/csb_project_setup.pub"
    cloud_init_file_path = "../cloud-init/loadgen-node-init.tpl"
    os_disk = {
      storage_account_type = "Premium_LRS"
      caching              = "ReadWrite"
      disk_size_gb         = 30
    }
    source_image = {
      publisher = "Canonical"
      offer     = "0001-com-ubuntu-server-jammy"
      sku       = "22_04-lts-gen2"
      version   = "latest"
    }
  }
}