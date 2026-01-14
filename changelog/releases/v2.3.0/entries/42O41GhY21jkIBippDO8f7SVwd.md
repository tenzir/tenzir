---
title: "Lower the impact of low-priority queries"
type: change
author: dominiklohmann
created: 2022-08-08T12:02:35Z
pr: 2484
---

We improved the operability of VAST servers under high load from automated
low-priority queries. VAST now considers queries issued with `--low-priority`,
such as automated retro-match queries, with even less priority compared to
regular queries (down from 33.3% to 4%) and internal high-priority queries used
for rebuilding and compaction (down from 12.5% to 1%).
