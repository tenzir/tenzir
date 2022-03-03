resource "aws_iam_role" "lambda_role" {
  name = "${module.env.tags["module"]}_${var.function_base_name}_${var.region_name}_${module.env.stage}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF

  tags = module.env.tags
}

resource "aws_iam_role_policy" "lambda_default_policy" {
  name = "${module.env.tags["module"]}_${var.function_base_name}_${var.region_name}_${module.env.stage}"
  role = aws_iam_role.lambda_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "${aws_cloudwatch_log_group.lambda_log_group.arn}",
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lamba_exec_role_eni" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "additional-attachments" {
  count = length(var.additional_policies)

  role       = aws_iam_role.lambda_role.name
  policy_arn = var.additional_policies[count.index]
}
