---
title: "Normalize pushed-up predicates in `subscribe`"
type: bugfix
author: dominiklohmann
created: 2025-02-25T12:26:53Z
pr: 5014
---

We fixed an optimization bug that caused pipelines of the form `subscribe
<topic> | where <value> in <field>` to evaluate the predicate `<field> in
<value>` instead, returning incorrect results from the pipeline.
