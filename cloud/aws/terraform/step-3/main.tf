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

resource "aws_cloudwatch_event_bus" "local_obj_event_bus" {
  name = "${module.env.module_name}-obj-events-${module.env.stage}"
}

resource "aws_cloudwatch_event_rule" "local_s3_object_events_rule" {
  name           = "${module.env.module_name}-local-${module.env.stage}"
  description    = "All s3 object created events"
  event_bus_name = aws_cloudwatch_event_bus.local_obj_event_bus.name

  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"]
}
EOF
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  arn            = var.vast_lambda_arn
  event_bus_name = aws_cloudwatch_event_bus.local_obj_event_bus.name
  rule           = aws_cloudwatch_event_rule.local_s3_object_events_rule.name
}

resource "aws_lambda_permission" "eventbridge_invoke_lambda" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.vast_lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.local_s3_object_events_rule.arn
}

module "source_trail" {
  source         = "../modules/s3notif"
  bucket_name    = var.cloudtrail_bucket_name
  region         = var.cloudtrail_bucket_region
  target_bus_arn = aws_cloudwatch_event_bus.local_obj_event_bus.arn
}
