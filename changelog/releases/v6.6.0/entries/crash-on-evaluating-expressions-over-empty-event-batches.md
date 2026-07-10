---
title: Crash on evaluating expressions over empty event batches
type: bugfix
authors:
  - IyeOnline
  - claude
prs:
  - 6427
created: 2026-07-06T15:58:12.2197Z
---

Tenzir no longer crashes when a pipeline evaluates an expression over an empty
batch of events. This could happen, for example, when an `accept_tcp` or
`accept_http` source received a probe or partially-sent connection and passed
the resulting empty batch into operators like `where`.
