---
title: "Fix missing options sometimes not causing an error"
type: bugfix
author: dominiklohmann
created: 2022-07-27T20:29:39Z
pr: 2470
---

Missing arguments for the `--plugins`, `--plugin-dirs`, and `--schema-dirs`
command line options no longer cause VAST to crash occasionally.
