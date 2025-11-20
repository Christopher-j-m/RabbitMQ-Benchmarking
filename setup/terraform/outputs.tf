# Console outputs after the deployment is complete.
# Prints the public IPs of all VMs and URLs for accessing the RabbitMQ management UI.


# Cluster Nodes Public IPs
output "cluster_public_ips" {
  description = "Public IPs for each RabbitMQ cluster node."
  value       = azurerm_public_ip.cluster_pip[*].ip_address
}

# Single Node Public IP (nullable)
output "single_node_ip" {
  description = "Public IP for the standalone RabbitMQ VM (null when disabled)."
  value       = length(azurerm_public_ip.single_pip) > 0 ? azurerm_public_ip.single_pip[0].ip_address : null
}

# Load Generator Public IPs
output "load_generator_ips" {
  description = "Public IPs for each load generator VM."
  value       = azurerm_public_ip.loadgen_pip[*].ip_address
}

# RabbitMQ Management UI URLs
output "rmq_management_ui" {
  description = "URLs for accessing the management UI on each provisioned RabbitMQ node."
  value = merge(
    { for idx, pip in azurerm_public_ip.cluster_pip : format("%s-%02d", var.cluster_nodes.name_prefix, idx + 1) => "http://${pip.ip_address}:15672/" },
    length(azurerm_public_ip.single_pip) > 0 ? { (var.single_node.name) = "http://${azurerm_public_ip.single_pip[0].ip_address}:15672/" } : {}
  )
}