---
title: "Keep from tcp pipelines running on connection failures"
type: bugfix
author: tobim
created: 2024-09-17T23:00:12Z
pr: 4602
---

Pipelines starting with `from tcp` no longer enter the failed state when an
error occurrs in one of the connections.
