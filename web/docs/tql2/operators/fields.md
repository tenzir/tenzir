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

```tql
fields
summarize field, count=count_distinct(schema), schemas=distinct(schema)
sort -count
head 5
```
