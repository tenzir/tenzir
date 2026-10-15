---
title: "Heterogeneous lists in served events"
type: bugfix
authors:
  - aljazerzen
---

Schema definitions, as reported by the `/serve` endpoints and by `measure
_exact_definition=true`, no longer describe the elements of a list as a
`union`, which no API consumer can express. Nulls are neutral, so `[1, null]`
is a `list<int64>` again, and records unify into the union of their fields, so
`[{a: 1}, {b: 2}]` is a `list<record{a: int64, b: int64}>`. Only lists whose
elements genuinely disagree, such as `[1, "x"]`, become a `list<string>`.

For such lists, the `/serve` endpoints stringify the elements of the served
events so that the data matches the definition. Nulls stay null. This affects
only the API; `write_json` and friends keep printing the values verbatim.
