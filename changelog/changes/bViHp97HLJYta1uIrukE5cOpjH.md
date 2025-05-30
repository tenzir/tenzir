---
title: "Fix bug that caused `read_zeek_tsv` to produce invalid fields"
type: bugfix
authors: dominiklohmann
pr: 5052
---

The `read_zeek_tsv` operator sometimes produced an invalid field with the name
`\0` for types without a schema specified. This no longer happens.
