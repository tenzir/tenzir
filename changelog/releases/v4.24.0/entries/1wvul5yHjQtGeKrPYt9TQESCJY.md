---
title: "Stop URL-encoding pub/sub topics"
type: change
author: dominiklohmann
created: 2024-11-07T20:30:02Z
pr: 4738
---

The topics provided to the `publish` and `subscribe` operators now exactly match
the `topic` field in the corresponding metrics.

Using `publish` and `subscribe` without an explicitly provided topic now uses
the topic `main` as opposed to an implementation-defined special name.
