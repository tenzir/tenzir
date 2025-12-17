---
title: "PRs 1407-1487-1490"
type: feature
author: tobim
created: 2021-03-04T08:27:48Z
pr: 1407
---

The schema language now supports 4 operations on record types: `+` combines the
fields of 2 records into a new record. `<+` and `+>` are variations of `+` that
give precedence to the left and right operand respectively. `-` creates a record
with the field specified as its right operand removed.
