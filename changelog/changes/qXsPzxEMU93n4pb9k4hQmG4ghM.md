---
title: "Make stop command blocking and return properly"
type: bugfix
authors: mavam
pr: 849
---

The `stop` command always returned immediately, regardless of whether it
succeeded. It now blocks until the remote node shut down properly or returns an
error exit code upon failure.
