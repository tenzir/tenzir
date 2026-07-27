---
title: Validate user-defined operator names in `tenzir.yaml`
type: bugfix
authors:
  - IyeOnline
prs:
  - 6455
created: 2026-07-17T11:24:50.537362Z
---

Operators defined in `tenzir.yaml` under `tenzir.operators` now have their
names validated the same way as operators defined in packages. Previously, an
invalid name such as `a-b` was silently accepted at startup even though the
resulting operator could never be referenced from a pipeline.
