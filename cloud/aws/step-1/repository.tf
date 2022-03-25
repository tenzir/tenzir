resource "aws_ecr_repository" "vast_lambda" {
  name                 = "${module.env.module_name}-lambda-${module.env.stage}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  lifecycle {
    ignore_changes = [tags["current"]]
  }
}
