---
title: Shutdown hangs and reports unresponsive pipelines
type: bugfix
authors:
  - aljazerzen
prs:
  - 6428
created: 2026-07-07T12:45:14.848087Z
---

Some internal pipelines and pipelines that use `every` operator, might not
shutdown correctly when the pipeline is stopped. This can stall the node
shutdown procedure and flood the logs with "logic error" messages.

Additionally, error diagnostics from pipelines that fail while stopping are no
longer lost. They now show up in the pipeline's diagnostics instead of a
generic `pipeline failed` message.
