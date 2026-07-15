---
title: Dynamic and multiple-pattern string replacement
type: feature
authors:
  - zedoraps
  - codex
prs:
  - 6449
created: 2026-07-15T11:48:18.725554Z
---

The `replace` function now accepts dynamic patterns and replacements, and the
new `replace_all` function replaces multiple literal strings in one call:

```tql
message = message.replace_all(
  [source_ip.string(), destination_ip.string()],
  "<redacted>",
)
```

`replace_all` requires a `list<string>` and propagates null pattern lists and
null list elements. Literal `replace` calls with an empty pattern leave the
subject unchanged.
