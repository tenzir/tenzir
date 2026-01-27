---
title: "Improve the `export` operator"
type: bugfix
author: dominiklohmann
created: 2024-02-05T18:49:33Z
pr: 3909
---

We fixed a bug that under rare circumstances led to an indefinite hang when
using a high-volume source followed by a slow transformation and a fast sink.
