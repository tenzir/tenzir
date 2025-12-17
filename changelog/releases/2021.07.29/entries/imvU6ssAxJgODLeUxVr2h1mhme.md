---
title: "Publish a tenzir/vast-dev Docker image"
type: feature
author: dominiklohmann
created: 2021-06-30T08:21:26Z
pr: 1749
---

VAST now comes with a
[`tenzir/vast-dev`](https://hub.docker.com/r/tenzir/vast-dev) Docker image in
addition to the regular [`tenzir/vast`](https://hub.docker.com/r/tenzir/vast).
The `vast-dev` image targets development contexts, e.g., when building
additional plugins. The image contains all build-time dependencies of VAST and
runs as `root` rather than the `vast` user.
