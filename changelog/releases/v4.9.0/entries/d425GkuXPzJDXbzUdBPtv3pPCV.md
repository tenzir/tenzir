---
title: "Gracefully handle misaligned header and values in `xsv` parser"
type: bugfix
author: dominiklohmann
created: 2024-02-04T20:42:33Z
pr: 3874
---

The `xsv` parser (and by extension the `csv`, `tsv`, and `ssv` parsers) skipped
 lines that had a mismatch between the number of values contained and the number
 of fields defined in the header. Instead, it now fills in `null` values for
 missing values and, if the new `--auto-expand` option is set, also adds new
 header fields for excess values.
