variable "bucket_name" {
}

variable "region" {
}

variable "target_bus_arn" {
}

module "env" {
  source = "../../common/env"
}

locals {
  # create a unique id so that this stack can be deployed multiple times
  id_raw = "${var.bucket_name}-${module.env.module_name}-${module.env.stage}-${var.target_bus_arn}"
  # 6 hexa digits should be more than sufficient to avoid conflicts as this stack
  # will be deployed only a very moderate amount of times within an account
  id = substr(md5(local.id_raw), 0, 6)
}

# Check that the provided buckets exists and is in the expected region
resource "null_resource" "bucket-location-check" {
  provisioner "local-exec" {
    command = "aws s3api get-bucket-location --bucket ${var.bucket_name} | grep ${var.region} || exit 1"
    when    = create
  }
}
