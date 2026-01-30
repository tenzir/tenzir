---
title: Correct multi-partition commits in `from_kafka`
type: bugfix
authors:
  - raxyte
  - codex
pr: 5654
created: 2026-01-12T13:28:04.709032Z
---

The `from_kafka` operator now commits offsets per partition and tracks partition
EOFs based on the current assignment, preventing premature exits and
cross-partition replays after restarts.
