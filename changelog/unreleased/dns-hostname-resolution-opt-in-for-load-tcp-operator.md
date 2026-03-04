---
title: DNS hostname resolution opt-in for load_tcp operator
type: change
authors:
  - tobim
  - codex
pr: 5865
created: 2026-03-04T16:03:26.882337Z
---

The `load_tcp` operator now makes DNS hostname resolution opt-in with the `resolve_hostnames` parameter (defaults to `false`).

Previously, the operator always attempted reverse DNS lookups for peer endpoints, which could fail in environments without working reverse DNS configurations. Now you can enable this behavior by setting `resolve_hostnames` to `true`:

```tql
load_tcp endpoint="0.0.0.0:5555" resolve_hostnames=true {
  read_json
}
```

When enabled and DNS resolution fails, the operator emits a warning diagnostic (once) instead of failing. This allows the operator to continue functioning in environments where reverse DNS is unavailable or unreliable.
