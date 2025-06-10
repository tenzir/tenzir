---
title: "Make `ip == subnet` and `string == pattern` commutative"
type: bugfix
authors: dominiklohmann
pr: 4280
---

`subnet == ip` and `pattern == string` predicates now behave just like `ip ==
subnet` and `string == pattern` predicates.
