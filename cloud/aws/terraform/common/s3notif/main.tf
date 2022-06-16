# To enable the matching of object-level API operations on your Amazon S3
# buckets in Amazon EventBridge, you must use Amazon CloudTrail to set up and
# configure a trail to receive these events. This should be very cheap as we are
# only enabling the S3 object level trail on the target Cloudtrail output
# bucket.

# the bucket with the trail might be in another region
# In that case, the origin event rule needs to be in that region
provider "aws" {
  alias  = "origin_region"
  region = var.region
  default_tags {
    tags = {
      module      = module.env.module_name
      provisioner = "terraform"
      stage       = terraform.workspace
    }
  }
}

data "aws_caller_identity" "current" {
  provider = aws.origin_region
}

resource "aws_s3_bucket" "object_event_target" {
  provider      = aws.origin_region
  bucket        = "${var.bucket_name}-s3notif-${local.id}"
  force_destroy = true
  lifecycle {
    ignore_changes = [lifecycle_rule]
  }
}

resource "aws_cloudtrail" "object_events" {
  provider              = aws.origin_region
  name                  = "${module.env.module_name}-s3notif-${local.id}"
  s3_bucket_name        = aws_s3_bucket.object_event_target.id
  is_multi_region_trail = false

  advanced_event_selector {
    name = "New Cloudtrail S3 objects"

    field_selector {
      field  = "eventCategory"
      equals = ["Data"]
    }

    field_selector {
      field       = "resources.ARN"
      starts_with = ["arn:aws:s3:::${var.bucket_name}/"]
    }

    field_selector {
      field  = "eventName"
      equals = ["PutObject"]
    }

    field_selector {
      field  = "resources.type"
      equals = ["AWS::S3::Object"]
    }
  }

  depends_on = [aws_s3_bucket_policy.object_event_target]
}

resource "aws_s3_bucket_policy" "object_event_target" {
  provider = aws.origin_region
  bucket   = aws_s3_bucket.object_event_target.id
  policy   = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AWSCloudTrailAclCheck",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:GetBucketAcl",
            "Resource": "${aws_s3_bucket.object_event_target.arn}"
        },
        {
            "Sid": "AWSCloudTrailWrite",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": "${aws_s3_bucket.object_event_target.arn}/*"
        }
    ]
}
EOF
}

resource "aws_s3_bucket_lifecycle_configuration" "object_event_target" {
  provider = aws.origin_region
  bucket   = aws_s3_bucket.object_event_target.id
  rule {
    expiration {
      days = 1
    }
    id     = "expire-all-objects"
    status = "Enabled"
  }
}

resource "aws_cloudwatch_event_rule" "origin_s3_object_events_rule" {
  provider    = aws.origin_region
  name        = "${module.env.module_name}-s3notif-origin-${local.id}"
  description = "Capture s3 object created events"

  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["${var.bucket_name}"]
    }
  }
}
EOF
}

resource "aws_cloudwatch_event_target" "local_obj_event_bus_target" {
  provider = aws.origin_region
  arn      = var.target_bus_arn
  rule     = aws_cloudwatch_event_rule.origin_s3_object_events_rule.name
  role_arn = aws_iam_role.event_bus_invoke_remote_event_bus.arn
}
