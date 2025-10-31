variable "location" {
  description = "The Azure region where the VMs will be created"
  type        = string
  default     = "germanywestcentral"
}

variable "vm_size" {
  description = "The VM size for all RabbitMQ nodes"
  type        = string
  default     = "Standard_D2s_v5"
}

variable "admin_username" {
  description = "Administrator username for the VMs"
  type        = string
  default     = "admin"
}

variable "admin_ssh_key_path" {
  description = "Path to the SSH public key"
  type        = string
  default     = "PATH/TO/YOUR_KEY.pub"
}

variable "storage_account_type" {
  description = "The disk type for all the VMs OS disk"
  type        = string
  default     = "Premium_LRS"
}