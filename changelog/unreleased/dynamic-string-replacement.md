---
title: Dynamic string replacement
type: feature
authors:
  - zedoraps
  - codex
prs:
  - 6449
created: 2026-07-15T11:48:18.725554Z
---

The `replace` function now accepts dynamic patterns and replacements:

```tql
message = message.replace(user.name, "<redacted>")
```

Literal `replace` calls with an empty pattern leave the subject unchanged.
