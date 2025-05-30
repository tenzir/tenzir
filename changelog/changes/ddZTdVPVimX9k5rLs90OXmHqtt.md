---
title: "Keep from tcp pipelines running on connection failures"
type: bugfix
authors: tobim
pr: 4602
---

Pipelines starting with `from tcp` no longer enter the failed state when an
error occurrs in one of the connections.
