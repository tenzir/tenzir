---
title: "Fix splitting logic for heterogeneous evaluation"
type: bugfix
author: jachris
created: 2025-03-10T13:42:39Z
pr: 5043
---

Expressions that have varying output types for the same input types (mostly the
`parse_*` family of functions) no longer trigger an assertion on certain inputs.
