---
title: "Make `ip == subnet` and `string == pattern` commutative"
type: bugfix
author: dominiklohmann
created: 2024-06-06T15:00:22Z
pr: 4280
---

`subnet == ip` and `pattern == string` predicates now behave just like `ip ==
subnet` and `string == pattern` predicates.
