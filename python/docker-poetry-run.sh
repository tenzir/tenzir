#!/usr/bin/env bash

# Run a poetry script in an isolated Docker environment with the VAST binary and
# dependencies installed. 
#
# Args:
# - arguments to `poetry run`
# Env:
# - VAST_CONTAINER_REF: the container tag reference (default: latest)
# - VAST_CONTAINER_REGISTRY: the registry to source the base image (default: docker.io)

set -eux -o pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

DOCKER_BUILDKIT=1 docker build \
    --target vast-python-script \
    --build-arg VAST_VERSION=${VAST_CONTAINER_REF:-latest} \
    --build-arg VAST_CONTAINER_REGISTRY=${VAST_CONTAINER_REGISTRY:-docker.io} \
    --file ../docker/dev/Dockerfile \
    --tag tenzir/vast-python-script \
    $SCRIPT_DIR/..

docker run \
    --env VAST_CONSOLE_VERBOSITY=info \
    tenzir/vast-python-script \
    $@
