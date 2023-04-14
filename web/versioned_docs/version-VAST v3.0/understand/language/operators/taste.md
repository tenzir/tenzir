# taste

Limits the input to the first N results per unique schema.

## Synopsis

```
taste [LIMIT]
```

### Limit

An unsigned integer denoting how many events to keep per schema. Defaults to 10.

## Example

Get the first ten results of each unique schema.

```
taste
```

Get the first five results of each unique schema.

```
taste 5
```
