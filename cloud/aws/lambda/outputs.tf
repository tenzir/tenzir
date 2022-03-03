output "lambda_arn" {
    value = aws_lambda_function.lambda.arn
}

output "lambda_name" {
    value = aws_lambda_function.lambda.function_name
}

output "role_arn" {
    value = aws_iam_role.lambda_role.arn
}

output "log_group_name" {
    value = aws_cloudwatch_log_group.lambda_log_group.name
}

output "log_group_arn" {
    value = aws_cloudwatch_log_group.lambda_log_group.arn
}
