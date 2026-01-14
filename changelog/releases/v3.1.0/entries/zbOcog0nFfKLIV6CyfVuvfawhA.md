---
title: "Remove configuration-defined import/export pipelines"
type: change
author: dominiklohmann
created: 2023-04-03T11:32:29Z
pr: 3052
---

As already announced with the VAST v3.0 release, the `vast.pipeline-triggers`
option now no longer functions. The feature will be replaced with node
ingress/egress pipelines that fit better into a multi-node model than the
previous feature that was built under the assumption of a client/server model
with a single server.
