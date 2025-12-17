---
title: "Backport bug fixes for a v1.1.1 release"
type: bugfix
author: dominiklohmann
created: 2022-03-25T16:45:55Z
pr: 2160
---

The disk monitor now correctly continues deleting until below the low water mark
after a partition failed to delete.

We fixed a rarely occurring race condition caused query workers to become stuck
after delivering all results until the corresponding client process terminated.

Queries that timed out or were externally terminated while in the query backlog
and with more than five unhandled candidate partitions no longer permanently get
stuck.
