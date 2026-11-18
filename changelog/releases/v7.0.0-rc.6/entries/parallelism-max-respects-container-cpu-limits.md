---
title: Parallelism max respects container CPU limits
type: bugfix
authors:
  - jachris
created: 2026-11-14T15:08:59.667947Z
---

A degree of parallelism of `max` now respects the CPU limit of a container.
Previously, it used the number of CPUs of the host, so a container limited to 2
CPUs on a 64-core host ran 64 instances of every parallelizable operator. Now
`max` is the number of CPUs that the process may run on, capped by the CPU
quota of its cgroup and rounded up.
