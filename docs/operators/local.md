---
title: local
---

Forces a pipeline to run locally.

```tql
local { … }
```

## Description

The `local` operator takes a pipeline as an argument and forces it to run at a
client process.

This operator has no effect when running a pipeline through the API or Tenzir
Platform.

## Examples

### Do an expensive sort locally

```tql
export
where @name.starts_with("suricata")
local {
  sort timestamp
}
write_ndjson
save_file "eve.json"
```

## See Also

[`remote`](/reference/operators/remote)
