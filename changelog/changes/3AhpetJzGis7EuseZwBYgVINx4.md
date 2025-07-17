---
title: "Fix race condition with exporter timeouts"
type: bugfix
authors: lava
pr: 2165
---

Terminating or timing out exports during the catalog lookup no longer causes
query workers to become stuck indefinitely.
