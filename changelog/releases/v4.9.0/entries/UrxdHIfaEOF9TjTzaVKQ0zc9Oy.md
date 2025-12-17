---
title: "Make `/serve` more consistent"
type: bugfix
author: dominiklohmann
created: 2024-01-31T09:49:41Z
pr: 3885
---

The `/serve` API sometimes returned an empty string for the next continuation
token instead of `null` when there are no further results to fetch. It now
consistently returns `null`.
