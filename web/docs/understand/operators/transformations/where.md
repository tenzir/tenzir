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
