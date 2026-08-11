---
title: Spurious unreachable warnings when deleting pipelines
type: bugfix
authors:
  - aljazerzen
  - claude
created: 2026-08-11T13:19:32.727563Z
---

Deleting or stopping pipelines no longer produces spurious
`received down message for unknown pipeline: !! unreachable` warnings in
the node log, and the affected pipelines are no longer marked as failed
with a bogus `unreachable` diagnostic. Creating and deleting many
pipelines in quick succession previously made these messages a steady
stream of noise.
