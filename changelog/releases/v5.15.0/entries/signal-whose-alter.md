---
title: "Octet Counting in `read_syslog`"
type: feature
author: IyeOnline
created: 2025-09-16T16:44:23Z
pr: 5472
---

We have added a new option `octet_counting` to the `read_syslog` operator.
Enabling this option will determine messages boundaries according to [RFC6587](https://datatracker.ietf.org/doc/html/rfc6587#section-3.4.1)
instead of our heuristic.
