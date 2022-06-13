resource "aws_iam_policy" "cloudtrail_s3_get" {
  name   = "${module.env.module_name}-cloutrail-s3-${local.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::${var.cloudtrail_bucket_name}/*"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "cloudtrail_s3_get" {
  role       = var.vast_lambda_role_name
  policy_arn = aws_iam_policy.cloudtrail_s3_get.arn
}
