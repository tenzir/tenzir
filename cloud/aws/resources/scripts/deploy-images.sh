#!/bin/bash

# Deploy the images and create the tfvars file containing their url.
# Unfortunately, this is required to get the output of a Terragrunt before-hook
# as an input to Terraform.

echo "arg1    (repo arn)    : " $1 
echo "arg2..N (image types) : " ${@:2}

set -e

IMAGE_TFVARS_FILE=images.generated.tfvars
rm -f $IMAGE_TFVARS_FILE
for var in "${@:2}"
do
    ../../vast-cloud docker-login deploy-image --type $var
    echo "${var}_image = \"$(../../vast-cloud current-image --repo-arn $1 --type $var)\"" >> $IMAGE_TFVARS_FILE
done
