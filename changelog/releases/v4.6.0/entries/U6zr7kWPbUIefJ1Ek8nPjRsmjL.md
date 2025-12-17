---
title: "Add `from/load/to/save <uri/file>`"
type: feature
author: eliaskosunen
created: 2023-11-17T15:37:31Z
pr: 3608
---

The operators `from`, `to`, `load`, and `save` support using URLs and file paths
directly as their argument. For example, `load https://example.com` means
`load https https://example.com`, and `save local-file.json` means
`save file local-file.json`.
