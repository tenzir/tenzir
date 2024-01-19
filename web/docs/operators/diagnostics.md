---
sidebar_custom_props:
  operator:
    source: true
---

# diagnostics

Retrieves diagnostic events from a Tenzir node.

## Synopsis

```
diagnostics [--live]
```

## Description

The `diagnostics` operator retrieves diagnostic events from a Tenzir
node.

### `--live`

Work on all diagnostic events as they are generated in real-time instead of on
diagnostic events persisted at a Tenzir node.

## Schemas

Tenzir emits diagnostic information with the following schema:

### `tenzir.diagnostic`

Contains detailed information about the diagnostic.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline that received the diagnostic.|
|`timestamp`|`time`|The exact timestamp of the diagnostic creation.|
|`message`|`string`|The diagnostic message.|
|`severity`|`string`|The diagnostic severity.|
|`notes`|`list<record>`|The diagnostic notes. Can be empty.|
|`annotations`|`list<record>`|The diagnostic annotations. Can be empty.|

## Examples

View all diagnostics generated in the past five minutes.

```
diagnostics
| where timestamp > 5 minutes ago
```

Only show diagnostics that contain the `error` severity.

```
diagnostics
| where severity == "error"
```
