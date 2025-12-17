---
title: "Implement `ip in subnet` and `subnet in subnet`"
type: feature
author: jachris
created: 2024-12-06T15:53:24Z
pr: 4841
---

Whether an IP address is contained in a subnet can now be checked using
expressions such as `1.2.3.4 in 1.2.0.0/16`. Similarly, to check whether a
subnet is included in another subnet, use `1.2.0.0/16 in 1.0.0.0/8`.
