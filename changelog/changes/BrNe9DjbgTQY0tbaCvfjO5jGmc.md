---
title: "Add a `--omit-nulls` option to the JSON export"
type: feature
authors: dominiklohmann
pr: 2004
---

The new `--omit-nulls` option to the `vast export json` command causes VAST to
skip over fields in JSON objects whose value is `null` when rendering them.
