---
title: "Fix reconnect attempts for remote pipelines"
type: bugfix
authors: dominiklohmann
pr: 3188
---

Starting a remote pipeline with `vast exec` failed when the node was not
reachable yet. Like other commands, executing a pipeline now waits until the
node is reachable before starting.
