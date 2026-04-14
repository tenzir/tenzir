---
title: Include pipeline names in diagnostics and metrics
type: feature
authors:
  - IyeOnline
  - claude
pr: 5959
created: 2026-03-30T12:28:14.671176Z
---

The `metrics` and `diagnostics` operators now include a `pipeline_name` field.

Previously, output from these operators only identified the source pipeline by its ID.
Now the human-readable name is available too, making it straightforward to filter
or group results by pipeline name without needing to look up IDs separately.

Please keep in mind that pipeline names are not unique.
