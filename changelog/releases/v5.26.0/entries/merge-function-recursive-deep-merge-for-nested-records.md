---
title: merge() function recursive deep merge for nested records
type: bugfix
authors:
  - mavam
  - claude
pr: 5728
created: 2026-02-05T15:56:10.953688Z
---

The `merge()` function now performs a recursive deep merge when merging two records. Previously, nested fields were dropped when merging, so `merge({hw: {sn: "XYZ123"}}, {hw: {model: "foobar"}})` would incorrectly produce `{hw: {model: "foobar"}}` instead of recursively merging the nested fields. The function now correctly produces `{hw: {sn: "XYZ123", model: "foobar"}}` by materializing both input records and performing a deep merge on them.
