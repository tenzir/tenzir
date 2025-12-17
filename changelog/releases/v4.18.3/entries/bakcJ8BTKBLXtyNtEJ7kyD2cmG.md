---
title: "Prevent unbounded memory usage in `export --live`"
type: bugfix
author: dominiklohmann
created: 2024-07-16T15:57:42Z
pr: 4396
---

We fixed a bug that caused a potentially unbounded memory usage in `export
--live`, `metrics --live`, and `diagnostics --live`.
