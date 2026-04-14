---
title: Unified context lookups with `context::lookup` operator
type: feature
authors:
  - IyeOnline
pr: 5964
created: 2026-04-01T15:50:31.647438Z
---

The `context::lookup` operator enables unified matching of events against contexts
by combining live and retrospective filtering in a single operation.

The operator automatically translates context updates into historical queries
while simultaneously filtering all newly ingested data against any context updates.

This provides:

- **Live matching**: Filter incoming events through a context with `live=true`
- **Retrospective matching**: Apply context updates to historical data with `retro=true`
- **Unified operation**: Use both together (default) to match all events—new and historical

Example usage:
```tql
context::lookup "feodo", field=src_ip
where @name == "suricata.flow"
```
