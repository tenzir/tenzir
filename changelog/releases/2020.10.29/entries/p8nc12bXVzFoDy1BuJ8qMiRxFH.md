---
title: "Don't enable automatic connections by default"
type: bugfix
author: lava
created: 2020-10-23T10:56:08Z
pr: 1110
---

VAST no longer opens a random public port, which used to be enabled in the
experimental VAST cluster mode in order to transparently establish a full mesh.
