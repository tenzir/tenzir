---
title: TQL type syntax for package UDOs
type: breaking
authors:
  - mavam
  - codex
prs:
  - 6242
created: 2026-05-31T06:44:24.431435Z
---

Package UDO argument types now use TQL type names, such as `int`, `uint`, and
`float`, instead of legacy names such as `int64`, `uint64`, and `double`. List
types use TQL syntax, for example `type: "[int]"` in YAML.

For example, a package operator argument like this now materializes `$limit`
as `int64`:

```yaml
args:
  named:
    - name: limit
      type: int
      default: 1000
```
