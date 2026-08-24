---
title: Rebuilder crash after a failed parallel rebuild batch
type: bugfix
authors:
  - tobim
created: 2026-08-24T09:10:03.628332Z
---

The rebuilder no longer crashes with `called Option::operator-> on a None
value` when one of its parallel workers fails mid-run. Previously, a single
failing batch—for example, a partition transform aborting because its memory
budget was exhausted—ended the run while sibling workers still had follow-up
work queued, and the next queued message brought down the rebuilder until the
node was restarted. Automatic rebuilds silently stopped for the remainder of
the node's lifetime. The stale follow-up work is now discarded and the
rebuilder stays available for subsequent runs.
