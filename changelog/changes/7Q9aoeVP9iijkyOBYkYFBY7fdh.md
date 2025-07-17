---
title: "Let empty queries export everything"
type: feature
authors: dominiklohmann
pr: 1879
---

The query argument to the export and count commands may now be omitted, which
causes the commands to operate on all data. Note that this may be a very
expensive operation, so use with caution.
