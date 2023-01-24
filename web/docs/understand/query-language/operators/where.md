# where

Keeps rows matching the configured expression and removes the rest from the
input.

## Parameters

- `expression: string`: The expression to evaluate when matching rows.

## Example

```yaml
where:
  expression: "dest_port == 53"
```

## Pipeline Operator String Syntax (Experimental)

```
where EXPRESSION
```

### Example

Evaluate the expression `dest_port == 53`.

```
where dest_port == 53
```
