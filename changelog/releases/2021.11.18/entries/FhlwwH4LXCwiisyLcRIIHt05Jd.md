---
title: "Automatically add the skip attribute to records in lists"
type: bugfix
author: tobim
created: 2021-10-28T14:50:54Z
pr: 1933
---

VAST no longer tries to create indexes for fields of type `list<record{...}>` as
that wasn't supported in the first place.
