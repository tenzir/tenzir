---
title: "Port `deduplicate` to TQL2"
type: feature
author: dominiklohmann
created: 2024-12-13T11:53:48Z
pr: 4850
---

The `deduplicate` operator in TQL2 to help you remove events with a common key.
The operator provides more flexibility than its TQL1 pendant by letting the
common key use any expression, not just a field name. You can also control
timeouts with finer granularity.
