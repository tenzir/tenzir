---
title: OCSF enum list derivation
type: feature
authors:
  - jachris
  - mavam
  - codex
pr: 5354
created: 2026-04-30T16:47:58.705871Z
---

`ocsf::derive` now derives OCSF enum sibling fields for lists, not just scalar enum fields. For example, DNS answers with `flag_ids: [1, 3, 4]` now also get `flags: ["Authoritative Answer", "Recursion Desired", "Recursion Available"]`, and the reverse direction works for `flags` to `flag_ids` as well.
