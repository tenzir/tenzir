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
  name = "${module.env.module_name}-${var.source_name}-${module.env.stage}"

  tags = {
    description = "Notifications of new ${var.source_name} s3 objects"
  }
}

resource "aws_cloudwatch_event_rule" "local_s3_object_events_rule" {
  name           = "${module.env.module_name}-${var.source_name}-${module.env.stage}"
  description    = "Capture notifications of new ${var.source_name} s3 objects"
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

  input_transformer {
    input_paths = {
      objkey    = "$.detail.object.key",
    }
    input_template = <<EOF
{
  "cmd": "${base64encode(var.import_cmd)}",
  "env": {
    "SRC_KEY": <objkey>
  }
}
EOF
  }
}

resource "aws_lambda_permission" "eventbridge_invoke_lambda" {
  statement_id  = "AllowFrom${var.source_name}EventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.vast_lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.local_s3_object_events_rule.arn
}

module "source_trail" {
  source         = "../s3notif"
  bucket_name    = var.source_bucket_name
  region         = var.source_bucket_region
  target_bus_arn = aws_cloudwatch_event_bus.local_obj_event_bus.arn
}
