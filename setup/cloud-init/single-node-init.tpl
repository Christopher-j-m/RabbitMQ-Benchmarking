#cloud-config
package_update: true
package_upgrade: true
packages:
  - erlang-nox
  - rabbitmq-server
runcmd:
  - 'rabbitmq-plugins enable rabbitmq_management'
  - 'systemctl restart rabbitmq-server'
  - 'sleep 10'
  - 'rabbitmqctl add_user admin <admin_password>'
  - 'rabbitmqctl set_user_tags admin administrator'
  - 'rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"'