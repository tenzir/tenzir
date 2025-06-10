---
title: "Fix edge case when parsing nullable lists with type conflicts"
type: bugfix
authors: jachris
pr: 5134
---

Parsing of nullable lists with type conflicts could previously lead to an error
under very rare circumstances. This now works as expected.
