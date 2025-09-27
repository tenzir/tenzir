#!/usr/bin/env bash

# Run a uv command in an isolated Docker environment with the Tenzir binary and
# dependencies installed.
#
# Args:
# - arguments forwarded to `uv run` (for example, pass `--all-packages pytest`)
# Env:
# - TENZIR_CONTAINER_REF: the container tag reference (default: latest)
# - TENZIR_CONTAINER_REGISTRY: the registry to source the base image (default: docker.io)

set -eux -o pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

DOCKER_BUILDKIT=1 docker build \
  --build-arg TENZIR_VERSION=${TENZIR_CONTAINER_REF:-latest} \
  --build-arg TENZIR_CONTAINER_REGISTRY=${TENZIR_CONTAINER_REGISTRY:-docker.io} \
  --file "${SCRIPT_DIR}/Dockerfile" \
  --tag tenzir/tenzir-python-script \
  "${SCRIPT_DIR}/.."

docker run \
  --env TENZIR_CONSOLE_VERBOSITY=debug \
  --env TENZIR_PYTHON_INTEGRATION=1 \
  tenzir/tenzir-python-script \
  "$@"
