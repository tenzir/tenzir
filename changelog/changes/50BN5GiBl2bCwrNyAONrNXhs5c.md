---
title: "Implement `unroll` for records"
type: feature
authors: dominiklohmann
pr: 4934
---

The `unroll` operator now works for record fields as well as lists. The operator
duplicates the surrounding event for every field in the specified record.
