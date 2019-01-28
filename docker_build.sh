#!/bin/bash

set -x

docker build -t vast-io/vast:latest .
read -r VERSION<VERSION
docker tag vast-io/vast:latest vast-io/vast:${VERSION}
# Tag the builder stage
BUILDER=$(docker images --filter "label=builder=true" --format '{{.CreatedAt}}\t{{.ID}}' | sort -nr | head -n 1 | cut -f2)
docker tag ${BUILDER} vast-io/vast:builder
