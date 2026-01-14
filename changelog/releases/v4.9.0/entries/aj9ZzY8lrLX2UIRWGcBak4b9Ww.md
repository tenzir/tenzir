---
title: "Prevent duplicate fields in schema"
type: bugfix
author: jachris
created: 2024-02-12T14:36:05Z
pr: 3929
---

Invalid schema definitions, where a record contains the same key multiple times,
are now detected and rejected.
