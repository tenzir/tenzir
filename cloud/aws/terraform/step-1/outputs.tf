output "vast_lambda_repository_url" {
  value = aws_ecr_repository.vast_lambda.repository_url
}

output "vast_lambda_repository_arn" {
  value = aws_ecr_repository.vast_lambda.arn
}
