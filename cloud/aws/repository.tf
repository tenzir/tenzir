resource "time_static" "last_image_upload" {
  triggers = {
    "always_run" = timestamp()
  }
}

resource "aws_ecr_repository" "lambda" {
  name                 = "${module.env.module_name}-lambda-${module.env.stage}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }
}

resource "null_resource" "lambda_image_push" {
  triggers = {
    always_run = time_static.last_image_upload.rfc3339
  }

  # if local image does not exit, try to pull it from the public repository
  provisioner "local-exec" {
    command = <<EOT
      test ! "$(docker images -q ${module.env.vast_lambda_image}:${var.vast_version})" && docker pull ${module.env.vast_lambda_image}:${var.vast_version}
      docker tag "${module.env.vast_lambda_image}:${var.vast_version}" "${aws_ecr_repository.lambda.repository_url}:${time_static.last_image_upload.unix}"
      docker push "${aws_ecr_repository.lambda.repository_url}:${time_static.last_image_upload.unix}"
    EOT
  }
}
