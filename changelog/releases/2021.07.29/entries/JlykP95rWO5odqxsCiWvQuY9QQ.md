---
title: "Fix bug in bitmap offset computation"
type: bugfix
author: lava
created: 2021-07-08T09:57:52Z
pr: 1755
---

Queries against fields using a `#index=hash` attribute could have missed some
results. Fixing a bug in the offset calculation during bitmap processing
resolved the issue.
