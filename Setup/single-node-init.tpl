#cloud-config
package_update: true
package_upgrade: true
packages:
  - erlang-nox
  - rabbitmq-server
runcmd:
  - 'rabbitmq-plugins enable rabbitmq_management'
  - 'systemctl restart rabbitmq-server'