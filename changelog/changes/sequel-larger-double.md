---
title: "Add drop_nulls operator"
type: feature
authors: mavam
pr: 5370
---

The new `drop_nulls` operator removes fields containing null values from events. Without arguments, it drops all fields with null values. With field arguments, it drops only the specified fields if they contain null values.
