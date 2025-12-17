---
title: "PRs 1517-1656"
type: feature
author: lava
created: 2021-05-09T23:49:21Z
pr: 1517
---

The new *transforms* feature allows VAST to apply transformations to incoming
and outgoing data. A transform consists of a sequence of steps that execute
sequentially, e.g., to remove, overwrite, hash, encrypt data. A new plugin type
makes it easy to write custom transforms.
