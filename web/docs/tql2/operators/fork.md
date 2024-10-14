# fork

Executes a subpipeline with the same input.

```tql
fork { … }
```

## Description

Executes a subpipeline with the same input.

### `{ … }`

The pipeline to execute

## Examples

```tql
load_tcp "<url>"
fork { publish "raw-feed" }
where @name == "ocsf.dhcp_activity"
// Filter
```
