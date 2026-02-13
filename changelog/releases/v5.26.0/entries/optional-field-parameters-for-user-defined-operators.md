---
title: Optional field parameters for user-defined operators
type: feature
authors:
  - mavam
  - claude
pr: 5753
created: 2026-02-12T17:49:08.631507Z
---

User-defined operators in packages can now declare optional field-type parameters with `null` as the default value. This allows operators to accept field selectors that are not required to be provided.

When a field parameter is declared with `type: field` and `default: null`, you can omit the argument when calling the operator, and the parameter will receive a `null` value instead. You can then check whether a field was provided by comparing the parameter to `null` within the operator definition.

Example:

In your package's operator definition, declare an optional field parameter:

```yaml
args:
  named:
    - name: selector
      type: field
      default: null
```

In the operator implementation, check if the field was provided:

```tql
set result = if $selector != null then "field provided" else "field omitted"
```

When calling the operator, the field argument becomes optional:

```tql
my_operator                    # field is null
my_operator selector=x.y       # field is x.y
```

Only `null` is allowed as the default value for field parameters. Non-null defaults are rejected with an error during package loading.
