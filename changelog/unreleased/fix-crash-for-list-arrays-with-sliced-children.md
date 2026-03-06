---
title: Fix crash for list arrays with sliced children
type: change
authors:
  - mavam
  - codex
pr: 5873
created: 2026-03-06T16:27:30Z
---

Tenzir no longer crashes when appending Arrow list arrays whose child values array is already sliced.

This could happen when processing nested list data backed by Arrow arrays with offsets that still point into the original child storage. Tenzir now rebases these offsets before copying the list elements, which avoids out-of-bounds access and preserves the expected list contents.
