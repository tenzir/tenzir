# The load balancer could be factored out of this module if multiple
# Fargate services are used to avoid duplicating the NLB hourly costs.

resource "aws_lb_target_group" "fargate_target" {
  name        = "${module.env.module_name}-fargate-${local.id}"
  port        = var.port
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = var.vpc_id

  tags = {
    module_name = var.name
  }
}

resource "aws_lb" "fargate_network_lb" {
  name               = "${module.env.module_name}-fargate-${local.id}"
  internal           = true
  load_balancer_type = "network"

  subnet_mapping {
    subnet_id            = var.subnet_id
    private_ipv4_address = var.service_ip
  }

  tags = {
    module_name = var.name
  }
}

resource "aws_lb_listener" "fargate_listener" {
  load_balancer_arn = aws_lb.fargate_network_lb.id
  port              = var.port
  protocol          = "TCP"

  default_action {
    target_group_arn = aws_lb_target_group.fargate_target.id
    type             = "forward"
  }
}
