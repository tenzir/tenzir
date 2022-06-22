output "vast_lambda_repository_url" {
  value = aws_ecr_repository.vast_lambda.repository_url
}

output "vast_lambda_repository_arn" {
  value = aws_ecr_repository.vast_lambda.arn
}

output "vast_fargate_repository_url" {
  value = aws_ecr_repository.vast_server.repository_url
}

output "vast_fargate_repository_arn" {
  value = aws_ecr_repository.vast_server.arn
}
