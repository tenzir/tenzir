---
title: "PRs 1517-1656"
type: feature
authors: lava
pr: 1517
---

The new *transforms* feature allows VAST to apply transformations to incoming
and outgoing data. A transform consists of a sequence of steps that execute
sequentially, e.g., to remove, overwrite, hash, encrypt data. A new plugin type
makes it easy to write custom transforms.
