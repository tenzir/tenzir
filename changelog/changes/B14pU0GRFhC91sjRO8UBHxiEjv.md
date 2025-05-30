---
title: "Port `unroll` to TQL2"
type: feature
authors: dominiklohmann
pr: 4736
---

The `unroll` operator is now available in TQL2. It takes a field of type list,
and duplicates the surrounding event for every element of the list.
