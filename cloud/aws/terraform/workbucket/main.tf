provider "aws" {
  region = var.region_name
  default_tags {
    tags = {
      module      = module.env.module_name
      provisioner = "terraform"
      stage       = terraform.workspace
    }
  }
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "workbucket" {
  bucket        = "${module.env.module_name}-workbucket-${var.region_name}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_iam_policy" "s3_all" {
  name   = "${module.env.module_name}-workbucket-${var.region_name}-${data.aws_caller_identity.current.account_id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "*",
      "Resource": "arn:aws:s3:::${aws_s3_bucket.workbucket.id}/*"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "s3_all" {
  role       = var.vast_lambda_role_name
  policy_arn = aws_iam_policy.s3_all.arn
}
