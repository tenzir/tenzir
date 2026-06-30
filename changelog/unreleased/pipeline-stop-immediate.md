---
title: Pipelines acknowledge stop requests while draining
type: bugfix
authors:
  - aljazerzen
created: 2026-06-29T08:15:20.200606Z
---

Fixes a problem where stopping or updating a running pipeline would render a
node unresponsive and report `pipeline manager request ... timed out` error
messages. The pipeline manager now acknowledges the request immediately,
remembers that the pipeline is still draining, and finishes stopping (and, for
definition updates, restarting) in the background once the executor has
exited.
