---
title: Fix pipeline startup timeouts
type: bugfix
authors:
  - jachris
pr: 5893
created: 2026-03-11T00:00:00.000000Z
---

In some situations, pipelines could not be successfully started, leading to
timeouts and a non-responsive node, especially during node start.
