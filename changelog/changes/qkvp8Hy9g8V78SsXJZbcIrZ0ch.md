---
title: "Fix a bug that causes sources to stall"
type: bugfix
authors: dominiklohmann
pr: 1136
---

`vast import` no longer stalls when it doesn't receive any data for more than 10
seconds.
