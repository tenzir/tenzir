---
title: "Fix bug in bitmap offset computation"
type: bugfix
authors: lava
pr: 1755
---

Queries against fields using a `#index=hash` attribute could have missed some
results. Fixing a bug in the offset calculation during bitmap processing
resolved the issue.
