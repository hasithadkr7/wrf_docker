#!/usr/bin/env bash

rabbitmqctl add_user curw cfcwm07
rabbitmqctl add_vhost curwsl
rabbitmqctl set_user_tags curw administrator
rabbitmqctl set_permissions -p curwsl curw ".*" ".*" ".*"