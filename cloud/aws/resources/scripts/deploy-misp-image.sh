#!/bin/bash

# Deploy the image and create the tfvars file containing its url.
# Unfortunately, going through a tfvars file is required to get the output of a
# Terragrunt before-hook as an input to Terraform.

set -e

IMAGE_TFVARS_FILE=images.generated.tfvars
SERVICE=misp

rm -f $IMAGE_TFVARS_FILE
../../vast-cloud docker-login
BUILD_TAG=tenzir/vastcloud:$SERVICE
../../vast-cloud misp.build-image --tag="$BUILD_TAG"
../../vast-cloud deploy-image --service $SERVICE --tag="$BUILD_TAG"
echo "${SERVICE}_image = \"$(../../vast-cloud current-image --service $SERVICE)\"" >> $IMAGE_TFVARS_FILE
