# openapi

Shows the node's OpenAPI specification.

```tql
openapi
```

## Description

The `openapi` operator shows the current Tenzir node's [OpenAPI
specification](/api) for all available REST endpoint plugins.

## Examples

Render the OpenAPI specification as YAML:

```tql
openapi
write_yaml
```
