---
title: Fix `read_lines` operator for old executor
type: bugfix
authors:
  - tobim
created: 2026-02-17T19:12:46.666564Z
---

The `read_lines` operator was accidently broken while it was ported
to the new execution API. This change restores its functionality.
