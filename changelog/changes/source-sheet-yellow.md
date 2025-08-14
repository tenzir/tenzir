---
title: "Sorting Improvements"
type: change
authors: IyeOnline
pr: 5425
---

We have re-done the internals of the `sort` operator. You will now be able to
more reliably sort events using lists or records as keys. Lists are compared
lexicographically between their values, while records are compared by their
sorted key-value pairs.
