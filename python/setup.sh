#!/usr/bin/env bash

# Run this scrip to setup a running VAST container with the matcher plugin installed

set -e

ABSOLUTE_DIRNAME=$(dirname $(readlink -f  $0))

cd $ABSOLUTE_DIRNAME/../cloud/aws/

export VAST_VERSION=latest
export VAST_AWS_REGION=eu-west-1

export VASTCLOUD_REBUILD=1

./vast-cloud pro.login pro.pull-image

[[ $(docker ps -f "name=vast-pro" --format '{{.Names}}') == vast-pro ]] || \
   docker run --name vast-pro --rm -d tenzir/vast-pro:latest start
