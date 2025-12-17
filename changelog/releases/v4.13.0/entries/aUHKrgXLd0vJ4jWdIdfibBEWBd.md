---
title: "Fix a hang on shutdown and remove deprecated things"
type: change
author: dominiklohmann
created: 2024-05-06T08:19:05Z
pr: 4187
---

The deprecated `matcher` plugin no longer exists. Use the superior `lookup`
operator and contexts instead.

The deprecated `tenzir-ctl import` and `tenzir-ctl export` commands no longer
exists. They have been fully superseded by pipelines in the form `… | import`
and `export | …`, respectively.
