# where

Keeps rows matching the configured expression and removes the rest from the
input.

## Synopsis

```
where EXPRESSION
```

### Expression

The expression to evaluate when matching rows.

## Example

Evaluate the expression `dest_port == 53`.

```
where dest_port == 53
```

## YAML Syntax Example

:::info Deprecated
The YAML syntax is deprecated since VAST v3.0, and will be removed in a future
release. Please use the pipeline syntax instead.
:::

```yaml
where:
  expression: "dest_port == 53"
```
