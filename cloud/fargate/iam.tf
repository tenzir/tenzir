resource "aws_iam_role" "ecs_task_role" {
  name = "${module.env.module_name}_${var.name}_${module.env.stage}_${var.region_name}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF


  tags = module.env.default_tags
}
