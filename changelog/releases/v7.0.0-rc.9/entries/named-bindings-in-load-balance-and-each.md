---
title: Named bindings in load_balance and each
type: feature
authors:
  - mavam
created: 2026-11-26T11:26:01.286192Z
---

The `load_balance` and `each` operators now accept explicit `$name=<expr>` bindings, matching `group`:

```tql
load_balance $target=["192.168.0.30:8080", "192.168.0.30:8081"] {
  to_tcp $target { write_json }
}
```

```tql
each $id=case_id, $ip=ip, parallel=4 {
  from_file "events.ndjson" { read_ndjson }
  where src_ip == $ip
  case_id = $id
}
```

In `load_balance`, a named binding replaces the implicit `$over` variable. In `each`, one or more named bindings replace the implicit `$this` variable, keep enclosing bindings in scope when nesting `each`, and let the optimizer push upstream field projections when explicit field expressions replace `this`. The existing `$over` and `$this` shorthands remain supported.
