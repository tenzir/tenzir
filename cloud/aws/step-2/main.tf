# The default provider manages VAST resources and other monitoring appliances
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

# This provider will create enpoints in the existing VPC to allow the metric collection.
# It can be reconfigured to use other credentials to setup cross-account monitoring.
provider "aws" {
  alias  = "monitored_vpc"
  region = var.region_name
  default_tags {
    tags = {
      module      = module.env.module_name
      provisioner = "terraform"
      stage       = terraform.workspace
    }
  }
}

provider "time" {}

module "env" {
  source = "./env"
}
