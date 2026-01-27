---
title: "Rename count, int, real, and addr to uint64, int64, double, and ip respectively"
type: change
author: dominiklohmann
created: 2023-01-15T22:21:27Z
pr: 2864
---

The builtin types `count`, `int`, `real`, and `addr` were renamed to `uint64`,
`int64`, `double`, and `ip` respectively. For backwards-compatibility, VAST
still supports parsing the old type tokens in schema files.
