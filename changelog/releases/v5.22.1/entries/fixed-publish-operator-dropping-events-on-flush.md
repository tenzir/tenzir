---
title: Fixed `publish` operator dropping events
type: bugfix
authors:
  - raxyte
pr: 5618
created: 2025-12-18T16:50:32.640657Z
---

The `publish` operator could drop events during flush operations.
Events are now reliably delivered to subscribers.
