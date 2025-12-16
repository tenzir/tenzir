---
title: "Fixed an assertion failure in parsers"
type: bugfix
authors: IyeOnline
pr: 5590
---

We fixed a bug in a common component used across all parsers, which could enter
an inconsistent state, leading to an "unexpected internal error: unreachable".
