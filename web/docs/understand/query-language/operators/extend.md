# extend

Adds the configured fields with fixed values.

:::warning Unstable
We plan to merge the `extend` and `replace` operators into a single `put`
operator in the near future, removing the need for the `extend` operator.
:::

## Synopsis

```
extend FIELD=VALUE[, …]
```

### Fields

The fields to add with fixed values.

## Example

Add a field named `secret` with the string value `"xxx"`, a field named `ints`
with the list of integers value `[1, 2, 3]`, and a field named `strs` with the
list of strings value `["a", "b", "c"]`:

```
extend secret="xxx", ints=[1, 2, 3], strs=["a", "b", "c"]
```

## YAML Syntax Example

:::info Deprecated
The YAML syntax is deprecated since VAST v3.0, and will be removed in a future
release. Please use the pipeline syntax instead.
:::

```yaml
extend:
  fields:
    secret: xxx
    ints:
      - 1
      - 2
      - 3
```
