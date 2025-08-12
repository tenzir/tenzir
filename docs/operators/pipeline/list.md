---
title: pipeline::list
category: Pipelines, Packages
example: "pipeline::list"
---

Shows managed pipelines.

```tql
pipeline::list
```

## Description

The `pipeline::list` operator returns the list of all managed pipelines. Managed
pipelines are pipelines created through the [`/pipeline`
API](/reference/node/api), which includes all pipelines run through the Tenzir
Platform.

## Examples

### Count pipelines per state

```tql
pipeline::list
top state
```

```tql
{
  "state": "running",
  "count": 31
}
{
  "state": "failed",
  "count": 4
}
{
  "state": "stopped",
  "count": 2
}
```

### Show pipelines per package

```tql
pipeline::list
summarize package, names=collect(name)
```

```tql
{
  "package": "suricata-ocsf",
  "names": [
    "Suricata Flow to OCSF Network Activity",
    "Suricata DNS to OCSF DNS Activity",
    "Suricata SMB to OCSF SMB Activity",
    // …
  ]
}
```

## See Also

[`list`](/reference/operators/package/list),
[`run`](/reference/operators/pipeline/run)
