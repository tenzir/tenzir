---
title: "Make stop command blocking and return properly"
type: bugfix
author: mavam
created: 2020-04-29T10:16:51Z
pr: 849
---

The `stop` command always returned immediately, regardless of whether it
succeeded. It now blocks until the remote node shut down properly or returns an
error exit code upon failure.
