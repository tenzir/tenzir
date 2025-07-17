---
title: "Improve the `export` operator"
type: bugfix
authors: dominiklohmann
pr: 3909
---

We fixed a bug that under rare circumstances led to an indefinite hang when
using a high-volume source followed by a slow transformation and a fast sink.
