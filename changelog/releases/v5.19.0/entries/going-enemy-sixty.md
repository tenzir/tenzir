---
title: "`ocsf::cast` operator"
type: feature
author: raxyte
created: 2025-10-24T12:00:08Z
pr: 5502
---

The new `ocsf::cast` operator handles common schema transformations when working
with OCSF events, such as homogenizing events of the same OCSF type or
converting timestamps to integer counts to strictly adhere to the schema.
This also deprecates the less flexible `ocsf::apply` operator, which is now
equivalent to `ocsf::cast null_fill=true`.
