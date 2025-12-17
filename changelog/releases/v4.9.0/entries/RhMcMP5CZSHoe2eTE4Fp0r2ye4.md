---
title: "Add support for commas in seconds in the time data parser"
type: bugfix
author: eliaskosunen
created: 2024-02-08T13:50:50Z
pr: 3903
---

Commas are now allowed as subsecond separators in timestamps in TQL.
Previously, only dots were allowed, but ISO 8601 allows for both.
