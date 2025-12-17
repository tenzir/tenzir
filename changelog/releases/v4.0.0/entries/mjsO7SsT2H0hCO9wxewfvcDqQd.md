---
title: "Fix reconnect attempts for remote pipelines"
type: bugfix
author: dominiklohmann
created: 2023-06-02T08:14:46Z
pr: 3188
---

Starting a remote pipeline with `vast exec` failed when the node was not
reachable yet. Like other commands, executing a pipeline now waits until the
node is reachable before starting.
