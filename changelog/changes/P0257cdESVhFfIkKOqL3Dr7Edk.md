---
title: "Fix missing options sometimes not causing an error"
type: bugfix
authors: dominiklohmann
pr: 2470
---

Missing arguments for the `--plugins`, `--plugin-dirs`, and `--schema-dirs`
command line options no longer cause VAST to crash occasionally.
