---
title: "Port `unroll` to TQL2"
type: feature
author: dominiklohmann
created: 2024-11-07T14:44:55Z
pr: 4736
---

The `unroll` operator is now available in TQL2. It takes a field of type list,
and duplicates the surrounding event for every element of the list.
