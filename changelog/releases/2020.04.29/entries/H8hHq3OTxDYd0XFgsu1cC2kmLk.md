---
title: "Fix inequality port lookups"
type: bugfix
author: mavam
created: 2020-04-22T18:56:12Z
pr: 834
---

Queries of the form `x != 80/tcp` were falsely evaluated as `x != 80/? && x !=
?/tcp`. (The syntax in the second predicate does not yet exist; it only
illustrates the bug.) Port inequality queries now correctly evaluate `x != 80/?
|| x != ?/tcp`. E.g., the result now contains values like `80/udp` and `80/?`,
but also `8080/tcp`.
