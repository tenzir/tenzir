---
title: "Prevent duplicate fields in schema"
type: bugfix
authors: jachris
pr: 3929
---

Invalid schema definitions, where a record contains the same key multiple times,
are now detected and rejected.
