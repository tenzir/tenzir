resource "aws_security_group" "vast_lambda" {
  name        = "${module.env.module_name}-vast_lambda-${module.env.stage}"
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
    security_groups = [aws_security_group.vast_lambda.id]
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "server_efs" {
  name        = "${module.env.module_name}-server_efs-${module.env.stage}"
  description = "Allow inbound from VAST server"
  vpc_id      = module.network.new_vpc_id

  ingress {
    protocol        = "tcp"
    from_port       = 2049
    to_port         = 2049
    security_groups = [aws_security_group.vast_server.id]
  }
}
