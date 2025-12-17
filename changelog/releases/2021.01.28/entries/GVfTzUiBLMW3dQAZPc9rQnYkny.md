---
title: "Consolidate Dockerfiles"
type: change
author: 0snap
created: 2021-01-26T16:53:30Z
pr: 1294
---

The GitHub CI changed to Debian Buster and produces Debian artifacts instead of
Ubuntu artifacts. Similarly, the Docker images we provide on [Docker
Hub](https://hub.docker.com/r/tenzir/vast) use Debian Buster as base image. To
build Docker images locally, users must set `DOCKER_BUILDKIT=1` in the build
environment.
