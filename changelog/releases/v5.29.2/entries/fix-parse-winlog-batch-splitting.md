---
title: Fix parse_winlog batch splitting
type: bugfix
authors:
  - jachris
pr: 5901
created: 2026-03-13T00:00:00.000000Z
---

The `parse_winlog` function could fragment output into thousands of tiny
batches due to type conflicts in `RenderingInfo/Keywords`, where events with
one `<Keyword>` emitted a string but events with multiple emitted a list.
Additionally, `EventData` with unnamed `<Data>` elements is now always emitted
as a record with `_0`, `_1`, etc. as field names instead of a list.
