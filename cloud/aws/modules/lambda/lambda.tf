resource "aws_lambda_function" "lambda" {
  package_type  = "Image"
  image_uri     = var.docker_image
  function_name = "${module.env.module_name}-${var.function_base_name}-${module.env.stage}"
  role          = aws_iam_role.lambda_role.arn
  memory_size   = var.memory_size
  timeout       = var.timeout

  environment {
    variables = merge(
      {
        STAGE = module.env.stage
      },
      var.environment
    )
  }

  vpc_config {
    security_group_ids = [aws_security_group.lambda_sg.id]
    subnet_ids         = var.subnets
  }

  depends_on = [aws_iam_role_policy_attachment.lamba_exec_role_eni]
}

resource "aws_lambda_function_event_invoke_config" "lambda_conf" {
  function_name                = aws_lambda_function.lambda.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
}


resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.lambda.function_name}"
  retention_in_days = 14
}

resource "aws_security_group" "lambda_sg" {
  name        = "${module.env.module_name}-${var.function_base_name}-${module.env.stage}"
  description = "allow outbound access"
  vpc_id      = var.vpc_id

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}
