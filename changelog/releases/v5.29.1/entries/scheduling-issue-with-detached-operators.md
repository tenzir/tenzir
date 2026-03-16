---
title: Scheduling issue with detached operators
type: bugfix
author: lava
pr: 5895
created: 2026-03-16T17:25:02.484408Z
---

Fixed a scheduling issue introduced in v5.24.0 that could cause the node to
become unresponsive when too many pipelines using detached operators like
`from_udp` were deployed simultaneously.
