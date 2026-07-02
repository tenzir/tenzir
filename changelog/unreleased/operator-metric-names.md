---
title: Fix confusing operator names in metrics
type: bugfix
authors:
  - aljazerzen
prs:
  - 6402
created: 2026-07-01T00:00:00.000000Z
---

Operator metrics and profiling no longer report demangled C++ type names such
as `Discard<tenzir::table_slice>`. Instead, they use the operator's name as it
appears in TQL, for example `discard`.
