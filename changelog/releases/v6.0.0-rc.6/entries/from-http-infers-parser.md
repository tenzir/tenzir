---
title: '`from_http` infers response parsers'
type: feature
authors:
  - mavam
  - codex
created: 2026-05-15T00:00:00Z
---

The `from_http` operator now accepts requests without an explicit parser
subpipeline when Tenzir can infer the response format from the `Content-Type`
header or URL extension:

```tql
from_http "https://example.com/events.json"
```

Explicit parser subpipelines continue to take precedence over inferred formats.
