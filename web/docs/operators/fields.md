---
sidebar_custom_props:
  operator:
    source: true
---

# fields

Retrieves all fields stored at a node.

```tql
fields
```

## Description

The `fields` operator shows a list of all fields stored at a node across all
available schemas.

## Examples

See the top five fields counted by how many schemas they occur in:

```
fields
| summarize count=count_distinct(schema), schemas=distinct(schema) by field
| sort count desc
| head 5
```
