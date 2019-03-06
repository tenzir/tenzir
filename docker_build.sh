#!/bin/bash

set -x

docker build -t vast-io/vast:latest .
read -r VERSION<VERSION
docker tag vast-io/vast:latest vast-io/vast:${VERSION}
# Tag the builder stage
BUILDER=$(docker images --filter "label=builder=true" --format '{{.CreatedAt}}\t{{.ID}}' | sort -nr | head -n 1 | cut -f2)
docker tag ${BUILDER} vast-io/vast:builder
# Export the images to gzipped archives
docker save vast-io/vast:latest | gzip > vast-${VERSION}-docker.tar.gz
docker save vast-io/vast:builder | gzip > vast-builder-${VERSION}-docker.tar.gz
