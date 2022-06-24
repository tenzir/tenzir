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
  id_raw = "${var.bucket_name}-${module.env.module_name}-${module.env.stage}-${var.target_bus_arn}"
  id     = substr(md5(local.id_raw), 0, 6)
}

# Check that the provided buckets exists and is in the expected region
resource "null_resource" "bucket-location-check" {
  provisioner "local-exec" {
    command = <<EOT
aws s3api get-bucket-location --bucket ${var.bucket_name} \
  | grep ${var.region} \
  || (echo 'Wrong region for source bucket'; exit 1)
    EOT
    when    = create
  }
}
