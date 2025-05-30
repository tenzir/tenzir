---
title: "Support lists and null values and empty strings in XSV parser"
type: bugfix
authors: dominiklohmann
pr: 3687
---

The `csv`, `ssv`, and `tsv` parsers now correctly support empty strings, lists,
and null values.

The `tail` operator no longer hangs occasionally.
