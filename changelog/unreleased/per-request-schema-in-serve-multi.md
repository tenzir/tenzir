---
title: Per-request schema in /serve-multi
type: feature
authors:
  - lava
  - claude
created: 2026-06-25T00:00:00.000000Z
---

The `/serve-multi` REST endpoint now accepts a `schema` field on each entry in
`requests`, letting you choose the schema representation (`legacy`, `exact`, or
`never`) independently for every output stream in a single request. The
top-level `schema` still applies as the default for any stream that does not set
its own.
