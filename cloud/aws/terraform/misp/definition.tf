locals {
  placeholder_hostname = "placeholder-hostname-to-rewrite-in-nginx"
  log_config = {
    logDriver = "awslogs"
    options = {
      awslogs-group         = aws_cloudwatch_log_group.fargate_logging.name
      awslogs-region        = var.region_name
      awslogs-stream-prefix = "ecs"
    }
  }
  misp_container_name = "misp"
  container_definition = [
    {
      image     = var.misp_image
      name      = "misp-core"
      essential = true
      portMappings = [{
        containerPort = 50000
        hostPort      = 50000
        protocol      = "tcp"
        }, {
        // Use this for REST API only
        // MISP UI requires hostname rewrite proxy
        containerPort = 80
        hostPort      = 80
        protocol      = "tcp"
      }],
      mountPoints = [
        {
          sourceVolume  = "files",
          containerPath = "/var/www/MISP/app/files",
          readOnly      = false
        }
      ]
      user = "${local.misp_uid}:${local.misp_gid}"
      environment = [{
        name  = "HOSTNAME"
        value = "http://${local.placeholder_hostname}"
        }, {
        name  = "REDIS_FQDN"
        value = "localhost"
        }, {
        name  = "INIT"
        value = "true"
        }, {
        name  = "NOREDIR"
        value = "true"
        }, {
        name  = "CRON_USER_ID"
        value = "1"
        }, {
        name  = "MYSQL_HOST"
        value = "127.0.0.1"
        }, {
        name  = "MYSQL_USER"
        value = "demo"
        }, {
        name  = "MYSQL_PASSWORD"
        value = "demo"
        }, {
        name  = "MYSQL_DATABASE"
        value = "misp"
        }, {
        name  = "DISIPV6"
        value = "true"
        }, {
        name  = "WORKERS"
        value = "1"
        }, {
        name  = "MISP_ADMIN_USER"
        value = "demo@tenzir.com"
        }, {
        name  = "MISP_ADMIN_PASSWORD"
        value = "demo"
        }, {
        name  = "MISP_API_KEY"
        value = "demodemodemodemodemodemodemodemodemodemo"
      }]
      logConfiguration = local.log_config
      depends_on = [
        {
          containerName = "redis",
          condition     = "HEALTHY"
        },
        {
          containerName = "mysql",
          condition     = "HEALTHY"
        }
      ]
    },
    {
      image     = "coolacid/misp-docker:modules-${var.misp_version}"
      name      = "misp-modules"
      essential = true
      environment = [{
        name  = "REDIS_BACKEND"
        value = "localhost"
      }]
      logConfiguration = local.log_config
      depends_on = [
        {
          containerName = "redis",
          condition     = "HEALTHY"
        },
        {
          containerName = "mysql",
          condition     = "HEALTHY"
        }
      ]
    },
    {
      image     = "mysql:${local.mysql_version}"
      name      = "mysql"
      essential = true
      command : ["--default-authentication-plugin=mysql_native_password"]
      capabilities = {
        add = ["SYS_NICE"]
      }
      user = "${local.mysql_uid}:${local.mysql_gid}"
      mountPoints = [
        {
          sourceVolume  = "mysql",
          containerPath = "/var/lib/mysql",
          readOnly      = false
        }
      ]
      environment = [{
        name  = "MYSQL_USER"
        value = "demo"
        }, {
        name  = "MYSQL_PASSWORD"
        value = "demo"
        }, {
        name  = "MYSQL_ROOT_PASSWORD"
        value = "demo"
        }, {
        name  = "MYSQL_DATABASE"
        value = "misp"
      }]
      logConfiguration = local.log_config
    },
    {
      image            = "redis:${local.redis_version}"
      name             = "redis"
      essential        = true
      logConfiguration = local.log_config
    },
    {
      image     = var.misp_proxy_image
      name      = "misp-proxy"
      essential = true
      portMappings = [{
        containerPort = local.misp_proxy_port
        hostPort      = local.misp_proxy_port
        protocol      = "tcp"
      }]
      environment = [{
        name  = "NGINX_PLACEHOLDER_HOSTNAME"
        value = local.placeholder_hostname
        }, {
        name  = "NGINX_PORT"
        value = "8080"
        }, {
        name  = "NGINX_PROXY_PASS"
        value = "http://localhost:80"
      }]
      logConfiguration = local.log_config
    },
    {
      image     = "linuxserver/openssh-server:latest"
      name      = "ssh-server"
      essential = true
      portMappings = [{
        containerPort = 2222
        hostPort      = 2222
        protocol      = "tcp"
      }]
      environment = [{
        name  = "PUBLIC_KEY"
        value = tls_private_key.tunneling_key.public_key_openssh
        }, {
        name  = "DOCKER_MODS"
        value = "linuxserver/mods:openssh-server-ssh-tunnel"
        }, {
        name  = "USER_NAME"
        value = "tunneler"
      }]
      logConfiguration = local.log_config
    }
    # we don't instantiate the smtp server
  ]
}
