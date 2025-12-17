---
title: "Fix a bug that causes sources to stall"
type: bugfix
author: dominiklohmann
created: 2020-11-10T12:51:27Z
pr: 1136
---

`vast import` no longer stalls when it doesn't receive any data for more than 10
seconds.
