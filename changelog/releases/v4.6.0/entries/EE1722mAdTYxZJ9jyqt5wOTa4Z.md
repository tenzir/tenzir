---
title: "Support lists and null values and empty strings in XSV parser"
type: bugfix
author: dominiklohmann
created: 2023-11-30T16:42:09Z
pr: 3687
---

The `csv`, `ssv`, and `tsv` parsers now correctly support empty strings, lists,
and null values.

The `tail` operator no longer hangs occasionally.
