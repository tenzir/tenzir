---
title: Typed integer defaults for package UDOs
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6242
created: 2026-05-31T06:44:24.431435Z
---

Package UDO arguments declared as `int` now accept positive YAML defaults without being misidentified as `uint64`.

For example, a package operator argument like this now materializes `$limit` as `int64`:

```yaml
args:
  named:
    - name: limit
      type: int
      default: 1000
```
