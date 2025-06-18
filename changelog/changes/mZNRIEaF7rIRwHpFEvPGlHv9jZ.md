---
title: "Expand subnet value predicates"
type: change
authors: mavam
pr: 1373
---

The query normalizer interprets value predicates of type `subnet` more broadly:
given a subnet `S`, the parser expands this to the expression `:subnet == S ||
:addr in S`. This change makes it easier to search for IP addresses belonging to
a specific subnet.
