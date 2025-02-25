# pass

Does nothing with the input.

```tql
pass
```

## Description

The `pass` operator relays the input without any modification. Outside of
testing and debugging, it is only used when an empty pipeline needs to created,
as `{}` is a record, while `{ pass }` is a pipeline.

## Examples

### Forward the input without any changes

```tql
pass
```

### Do nothing every 10s

```tql
every 10s {
  pass
}
```
