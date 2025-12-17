---
title: "Fix bug that caused `read_zeek_tsv` to produce invalid fields"
type: bugfix
author: dominiklohmann
created: 2025-03-17T12:41:38Z
pr: 5052
---

The `read_zeek_tsv` operator sometimes produced an invalid field with the name
`\0` for types without a schema specified. This no longer happens.
