---
title: "Add value grouping to `chart` and remove `--title`"
type: change
author: jachris
created: 2024-04-15T12:29:24Z
pr: 4119
---

In the `chart` operator, unless otherwise specified, every field but the
first one is taken to be a value for the Y-axis, instead of just the second one.

If the value for `-x`/`--name` or `-y`/`--value` is explicitly specified, the
other one must now be too.

The `--title` option is removed from `chart`. Titles can instead be set directly
in the web interface.
