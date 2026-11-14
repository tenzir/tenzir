---
title: Package hints name the tenzir command
type: bugfix
authors:
  - mavam
created: 2026-11-13T17:39:39.869217Z
---

When a workspace's lock is unreadable, misses a dependency, or names a package
that is not fetched yet, the hint now tells you to run `tenzir package lock` or
`tenzir package fetch` instead of the `tenzir-platform` variants, matching the
unified `tenzir` command of Tenzir 7.
