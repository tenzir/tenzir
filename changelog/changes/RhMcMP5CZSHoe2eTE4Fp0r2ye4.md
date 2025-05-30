---
title: "Add support for commas in seconds in the time data parser"
type: bugfix
authors: eliaskosunen
pr: 3903
---

Commas are now allowed as subsecond separators in timestamps in TQL.
Previously, only dots were allowed, but ISO 8601 allows for both.
