---
title: "Let empty queries export everything"
type: feature
author: dominiklohmann
created: 2021-09-16T18:40:11Z
pr: 1879
---

The query argument to the export and count commands may now be omitted, which
causes the commands to operate on all data. Note that this may be a very
expensive operation, so use with caution.
