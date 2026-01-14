---
title: "Unroll the Zeek TSV header parsing loop"
type: feature
author: Dakostu
created: 2023-07-04T08:23:01Z
pr: 3291
---

The `zeek-tsv` parser sometimes failed to parse Zeek TSV logs, wrongly
reporting that the header ended too early. This bug no longer exists.
