---
title: VAST v1.1.1
description: VAST v1.1.1 - Compaction & Query Language Frontends
authors: dominiklohmann
date: 2022-03-25
tags: [release, compaction, query]
---

Dear community, we are excited to announce [VAST
v1.1.1][github-vast-release-new].

This release contains some important bug fixes on top of everything included in
the [VAST v1.1][github-vast-release-old] release.

[github-vast-release-new]: https://github.com/tenzir/vast/releases/tag/v1.1.1
[github-vast-release-old]: https://github.com/tenzir/vast/releases/tag/v1.1.0

<!--truncate-->

- The disk monitor now correctly continues deleting until below the low water
  mark after a partition failed to delete.
- We fixed a rarely occurring race condition that caused query workers to become
  stuck after delivering all results until the corresponding client process
  terminated.
- Queries that timed out or were externally terminated while in the query
  backlog that had more unhandled candidate than taste partitions no longer
  permanently get stuck. This critical bug caused VAST to idle permanently on
  the export path once all workers were stuck.

Thanks to [@norg](https://github.com/norg) for reporting the issues.
