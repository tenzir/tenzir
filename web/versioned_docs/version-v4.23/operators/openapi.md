---
sidebar_custom_props:
  operator:
    source: true
---

# openapi

Shows the node's OpenAPI specification.

## Synopsis

```
openapi
```

## Description

The `openapi` operator shows the current Tenzir node's [OpenAPI
specification](/api) for all available REST endpoint plugins.

## Examples

Render the OpenAPI specification as YAML:

```
openapi | write yaml
```
