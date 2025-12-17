---
title: "PRs 1987-1992"
type: feature
author: tobim
created: 2021-12-08T15:48:49Z
pr: 1987
---

Metrics events now optionally contain a metadata field that is a key-value
mapping of string to string, allowing for finer-grained introspection. For now
this enables correlation of metrics events and individual queries. A set of new
metrics for query lookup use this feature to include the query ID.
