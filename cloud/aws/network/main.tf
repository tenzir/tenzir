terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      version               = ">= 3.0"
      configuration_aliases = [aws, aws.monitored_vpc]
    }
  }
}

data "aws_caller_identity" "peer" {
  provider = aws.monitored_vpc
}

data "aws_region" "peer" {
  provider = aws.monitored_vpc
}

data "aws_vpc" "peer" {
  id = var.peered_vpc_id
}
