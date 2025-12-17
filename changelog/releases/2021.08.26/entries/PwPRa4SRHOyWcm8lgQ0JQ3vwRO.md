---
title: "Support quoted non-string fields in the CSV parser"
type: bugfix
author: dominiklohmann
created: 2021-08-20T09:43:45Z
pr: 1858
---

The CSV parser now correctly parses quoted fields in non-string types. E.g.,
`"127.0.0.1"` in CSV now successfully parsers when a matching schema contains
an `address` type field.
