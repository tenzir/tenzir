---
title: "Implement `unroll` for records"
type: feature
author: dominiklohmann
created: 2025-01-23T11:09:28Z
pr: 4934
---

The `unroll` operator now works for record fields as well as lists. The operator
duplicates the surrounding event for every field in the specified record.
