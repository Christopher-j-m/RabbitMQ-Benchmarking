# Console outputs after the deployment is complete.
# Prints the public IPs of all VMs and URLs for accessing the RabbitMQ management UI.


# Cluster Nodes Public IPs
output "cluster_node_1_ip" {
  description = "Public IP for cluster node 1"
  value       = azurerm_public_ip.cluster_pip[0].ip_address
}

output "cluster_node_2_ip" {
  description = "Public IP for cluster node 2"
  value       = azurerm_public_ip.cluster_pip[1].ip_address
}

output "cluster_node_3_ip" {
  description = "Public IP for cluster node 3"
  value       = azurerm_public_ip.cluster_pip[2].ip_address
}

# Single Node Public IP
output "single_node_ip" {
  description = "Public IP for the single node VM"
  value       = azurerm_public_ip.single_pip.ip_address
}

# RabbitMQ Management UI URLs
output "rmq_management_ui" {
  value = {
    "cluster_node_1" = "http://${azurerm_public_ip.cluster_pip[0].ip_address}:15672/"
    "cluster_node_2" = "http://${azurerm_public_ip.cluster_pip[1].ip_address}:15672/"
    "cluster_node_3" = "http://${azurerm_public_ip.cluster_pip[2].ip_address}:15672/"
    "single_node"    = "http://${azurerm_public_ip.single_pip.ip_address}:15672/"
  }
}