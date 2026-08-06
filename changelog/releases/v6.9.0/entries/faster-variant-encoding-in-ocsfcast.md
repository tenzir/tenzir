---
title: Faster variant encoding in ocsf::cast
type: change
authors:
  - tobim
created: 2026-07-30T15:47:59.925013Z
---

`ocsf::cast` with `encode_variants=true` now encodes free-form objects such as `unmapped` significantly faster, which substantially lowers CPU usage for pipelines that cast high-volume streams with populated `unmapped` fields.
