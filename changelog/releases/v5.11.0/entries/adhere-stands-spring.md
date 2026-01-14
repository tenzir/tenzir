---
title: "Context operator metrics"
type: bugfix
author: jachris
created: 2025-07-29T11:13:05Z
pr: 5383
---

The data flowing through the `context::` family of operators is no longer
counted as actual ingress and egress.
