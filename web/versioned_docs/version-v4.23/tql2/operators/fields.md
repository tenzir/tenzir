# fields

Retrieves all fields stored at a node.

```tql
fields
```

## Description

The `fields` operator shows a list of all fields stored at a node across all
available schemas.

## Examples

### Get the top-5 most frequently used fields across schemas

```tql
fields
summarize field, count=count_distinct(schema), schemas=distinct(schema)
sort -count
head 5
```
