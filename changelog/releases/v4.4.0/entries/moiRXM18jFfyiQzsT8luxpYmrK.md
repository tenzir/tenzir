---
title: "Upgrade remaining usages of the adaptive table slice builder"
type: bugfix
author: jachris
created: 2023-10-18T11:58:52Z
pr: 3582
---

The `csv` parsed (or more generally, the `xsv` parser) now attempts to parse
fields in order to infer their types.
