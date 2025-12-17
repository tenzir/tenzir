---
title: "Disable auto-vectorization in prebuilt Docker images"
type: change
author: dominiklohmann
created: 2021-07-16T10:18:07Z
pr: 1778
---

The `vast` binaries in our [prebuilt Docker
images](http://hub.docker.com/r/tenzir/vast) no longer contain AVX instructions
for increased portability. Building the image locally
continues to add supported auto-vectorization flags automatically.

The following new build options exist: `VAST_ENABLE_AUTO_VECTORIZATION`
enables/disables all auto-vectorization flags, and
`VAST_ENABLE_SSE_INSTRUCTIONS` enables `-msse`; similar options exist for SSE2,
SSE3, SSSE3, SSE4.1, SSE4.2, AVX, and AVX2.
