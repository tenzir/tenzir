---
title: Delimited byte output
type: feature
authors:
  - mavam
  - codex
created: 2026-05-28T07:31:26.099667Z
---

The new `write_delimited` operator writes one `string` or `blob` value per
event and appends a separator after each value:

```tql
from {data: "a"}, {data: "b"}
to_stdout {
  write_delimited data, "|"
}
```

This outputs:

```txt
a|b|
```

This lets you frame compact JSON, GELF over TCP with a null byte, or other
preformatted messages without adding escaping or serialization in the byte
writer.
