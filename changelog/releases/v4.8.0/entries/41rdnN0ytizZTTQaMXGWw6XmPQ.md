---
title: "Support concepts in more places"
type: feature
author: dominiklohmann
created: 2024-01-12T16:46:31Z
pr: 3812
---

Concepts are now supported in more places than just the `where` operator: All
operators and concepts that reference fields in events now support them
transparently. For example, it is not possible to enrich with a lookup table
against all source IP addresses defined in the concept `net.src.ip`, or to group
by destination ports across different schemas with the concept `net.dst.port`.
