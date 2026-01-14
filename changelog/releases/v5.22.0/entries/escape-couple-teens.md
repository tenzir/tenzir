---
title: "Fixed an assertion failure in parsers"
type: bugfix
author: IyeOnline
created: 2025-12-03T13:52:04Z
pr: 5590
---

We fixed a bug in a common component used across all parsers, which could enter
an inconsistent state, leading to an "unexpected internal error: unreachable".
