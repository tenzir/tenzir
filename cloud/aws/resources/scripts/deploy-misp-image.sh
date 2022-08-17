#!/bin/bash

# Deploy the images and create the tfvars file containing their url.
# Unfortunately, going through a tfvars file is required to get the output of a
# Terragrunt before-hook as an input to Terraform.

set -e

IMAGE_TFVARS_FILE=images.generated.tfvars
rm -f $IMAGE_TFVARS_FILE
../../vast-cloud docker-login
BUILD_TAG=tenzir/vastcloud:misp
../../vast-cloud misp.build-misp-image --tag="$BUILD_TAG"
../../vast-cloud deploy-image --service misp --tag="$BUILD_TAG"
echo "misp_image = \"$(../../vast-cloud current-image --service misp)\"" >> $IMAGE_TFVARS_FILE
