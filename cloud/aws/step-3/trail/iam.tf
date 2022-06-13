resource "aws_iam_role" "event_bus_invoke_remote_event_bus" {
  name               = "${module.env.module_name}-cross-event-bus-${local.id}-${module.env.stage}-${var.region}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
EOF

  inline_policy {
    name = "cross_region_put"

    policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["events:PutEvents"],
      "Resource": "${var.target_bus_arn}"
    }
  ]
}
EOF
  }
}
