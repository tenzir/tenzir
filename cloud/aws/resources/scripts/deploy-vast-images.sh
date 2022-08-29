#!/bin/bash

# Deploy the images and create the tfvars file containing their url.
# Unfortunately, going through a tfvars file is required to get the output of a
# Terragrunt before-hook as an input to Terraform. Each argument $arg will be
# used to:
# - build docker/vast/$arg.Dockerfile
# - deploy the result as the $arg service image
# - create a ${arg}_image entry in the tfvars file

set -e

IMAGE_TFVARS_FILE=images.generated.tfvars
rm -f $IMAGE_TFVARS_FILE
../../vast-cloud docker-login
for arg in "$@"
do
    BUILD_TAG=tenzir/vastcloud:$arg
    ../../vast-cloud build-vast-image --dockerfile=$arg.Dockerfile --tag="$BUILD_TAG"
    ../../vast-cloud deploy-image --service $arg --tag="$BUILD_TAG"
    echo "${arg}_image = \"$(../../vast-cloud current-image --service $arg)\"" >> $IMAGE_TFVARS_FILE
done
