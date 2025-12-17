---
title: "Fix race condition with exporter timeouts"
type: bugfix
author: lava
created: 2022-03-28T13:56:44Z
pr: 2165
---

Terminating or timing out exports during the catalog lookup no longer causes
query workers to become stuck indefinitely.
