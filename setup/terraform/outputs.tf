# Console outputs after the deployment is complete.
# Prints the public IPs of all VMs and URLs for accessing the RabbitMQ management UI.


# Cluster Nodes Public IPs
output "cluster_public_ips" {
  description = "Public IPs for each RabbitMQ cluster node."
  value       = azurerm_public_ip.cluster_pip[*].ip_address
}

# Load Generator Public IP
output "load_generator_ip" {
  description = "Public IP for the load generator VM."
  value       = azurerm_public_ip.loadgen_pip[0].ip_address
}

# RabbitMQ Management UI URLs
output "rmq_management_ui" {
  description = "URLs for accessing the management UI on each provisioned RabbitMQ node."
  value = { for idx, pip in azurerm_public_ip.cluster_pip : format("%s-%02d", var.cluster_nodes.name_prefix, idx + 1) => "http://${pip.ip_address}:15672/" }
}