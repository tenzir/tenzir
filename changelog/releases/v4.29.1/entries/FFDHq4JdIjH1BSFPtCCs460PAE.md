---
title: "Fix crash in the MSB in merging mode"
type: bugfix
author: IyeOnline
created: 2025-03-03T10:05:21Z
pr: 5028
---

We fixed a bug in the `read_xsv` and `parse_xsv` family of operators and
functions that caused the parser to fail unexpectedly when the data contained
a list (as specified through the list separator) for fields where the provided
`schema` did not expect lists.
