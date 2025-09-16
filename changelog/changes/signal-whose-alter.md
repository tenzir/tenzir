---
title: "Octet Counting in `read_syslog`"
type: feature
authors: IyeOnline
pr: 5472
---

We have added a new option `octet_counting` to the `read_syslog` operator.
Enabling this option will determine messages boundaries according to [RFC6587](https://datatracker.ietf.org/doc/html/rfc6587#section-3.4.1)
instead of our heuristic.
