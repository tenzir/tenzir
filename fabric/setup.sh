#!/usr/bin/env bash

set -e

ABSOLUTE_DIRNAME=$(dirname $(readlink -f  $0))

cd $ABSOLUTE_DIRNAME/../cloud/aws/

export VAST_VERSION=latest

./vast-cloud pro.login pro.pull-image 

docker run --name vast-pro --rm -d tenzir/vast-pro:latest start
