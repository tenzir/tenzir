---
title: "Disable dense indexes"
type: change
author: dominiklohmann
created: 2023-10-07T20:52:29Z
pr: 3552
---

Tenzir no longer builds dense indexes for imported events. Dense indexes
improved query performance at the cost of a higher memory usage. However, over
time the performance improvement became smaller due to other improvements in the
underlying storage engine.

Tenzir no longer supports models in taxonomies. Since Tenzir v4.0 they were only
supported in the deprecated `tenzir-ctl export` and `tenzir-ctl count` commands.
We plan to bring the functionality back in the future with more powerful
expressions in TQL.
