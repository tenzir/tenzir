---
title: "Add new query language plugin"
type: change
author: mavam
created: 2022-02-11T10:36:14Z
pr: 2074
---

VAST no longer attempts to intepret query expressions as Sigma rules
automatically. Instead, this functionality moved to a dedicated `sigma` query
language plugin that must explicitly be enabled at build time.
