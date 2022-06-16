resource "aws_iam_policy" "s3_get" {
  name   = "${module.env.module_name}-s3proc-${var.source_name}-${local.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::${var.source_bucket_name}/*"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "s3_get" {
  role       = var.vast_lambda_role_name
  policy_arn = aws_iam_policy.s3_get.arn
}
