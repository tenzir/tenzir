---
title: "Sorting Improvements"
type: change
author: IyeOnline
created: 2025-08-19T18:44:31Z
pr: 5425
---

We have re-done the internals of the `sort` operator. You will now be able to
more reliably sort events using lists or records as keys. Lists are compared
lexicographically between their values, while records are compared by their
sorted key-value pairs.
