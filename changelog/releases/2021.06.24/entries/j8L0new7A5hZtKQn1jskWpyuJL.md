---
title: "Add option for configurable post-start hooks"
type: feature
author: dominiklohmann
created: 2021-06-02T10:34:11Z
pr: 1699
---

The new option `vast.start.commands` allows for specifying an ordered list of
VAST commands that run after successful startup. The effect is the same as first
starting a node, and then using another VAST client to issue commands.  This is
useful for commands that have side effects that cannot be expressed through the
config file, e.g., starting a source inside the VAST server that listens on a
socket or reads packets from a network interface.
