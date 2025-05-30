---
title: "Prevent unbounded memory usage in `export --live`"
type: bugfix
authors: dominiklohmann
pr: 4396
---

We fixed a bug that caused a potentially unbounded memory usage in `export
--live`, `metrics --live`, and `diagnostics --live`.
