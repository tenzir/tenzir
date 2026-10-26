---
title: Hang when reading stdin from a redirected file
type: bugfix
authors:
  - mavam
created: 2026-10-01T08:11:31.865861Z
---

Pipelines that read stdin now finish when stdin is redirected from a file:

```sh
tenzir 'from_stdin { read_json }' < events.json
```

Previously, such pipelines hung after reading the file on macOS, which also
affected `/dev/null` and pipelines with an implicit stdin source, such as
`tenzir 'where x > 1' < events.json`. Piping into `tenzir` worked and is
unaffected.
