---
title: "Correct check for user schema in zeek reader"
type: bugfix
authors: tobim
pr: 847
---

The parser for Zeek tsv data used to ignore attributes that were defined for the
Zeek-specific types in the schema files. It has been modified to respect and
prefer the specified attributes for the fields that are present in the input
data.
