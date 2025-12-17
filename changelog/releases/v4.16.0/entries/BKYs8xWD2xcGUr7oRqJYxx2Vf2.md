---
title: "Enable selective deletion of lookup table entries"
type: feature
author: dominiklohmann
created: 2024-06-05T07:13:17Z
pr: 4274
---

For `lookup-table` contexts, the new `--erase` option for `context update`
enables selective deletion of lookup table entries.

The `context update` operator now defaults the `--key <field>` option to the
first field in the input when no field is explicitly specified.
