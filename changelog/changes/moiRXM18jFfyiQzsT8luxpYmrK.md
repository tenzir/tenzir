---
title: "Upgrade remaining usages of the adaptive table slice builder"
type: bugfix
authors: jachris
pr: 3582
---

The `csv` parsed (or more generally, the `xsv` parser) now attempts to parse
fields in order to infer their types.
