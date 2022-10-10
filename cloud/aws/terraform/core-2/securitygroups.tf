resource "aws_security_group" "vast_client" {
  name        = "${module.env.module_name}-vast_client-${module.env.stage}"
  description = "Allow outbound access only"
  vpc_id      = module.network.new_vpc_id

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}


resource "aws_security_group" "vast_server" {
  name        = "${module.env.module_name}-vast_server-${module.env.stage}"
  description = "Allow access from VAST Lambda and allow all outbound traffic"
  vpc_id      = module.network.new_vpc_id

  ingress {
    protocol        = "tcp"
    from_port       = local.vast_port
    to_port         = local.vast_port
    security_groups = [aws_security_group.vast_client.id]
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "client_efs" {
  name        = "${module.env.module_name}-client_efs-${module.env.stage}"
  description = "Allow access to EFS"
  vpc_id      = module.network.new_vpc_id
}

resource "aws_security_group" "server_efs" {
  name        = "${module.env.module_name}-server_efs-${module.env.stage}"
  description = "Allow inbound from efs clients"
  vpc_id      = module.network.new_vpc_id

  ingress {
    protocol        = "tcp"
    from_port       = 2049
    to_port         = 2049
    security_groups = [aws_security_group.client_efs.id]
  }
}

resource "aws_security_group" "http_app_client" {
  name        = "${module.env.module_name}-http_app_client-${module.env.stage}"
  description = "Give network access to all apps"
  vpc_id      = module.network.new_vpc_id
}
