---
title: "User-defined operators support arguments and options"
type: feature
authors: tobim
---

User-defined operators in packages can now declare arguments and options in
their YAML frontmatter, enabling parameterized operator definitions with the
same calling convention as built-in operators.

Arguments are positional parameters, while options are named parameters with
optional default values. Both support constant values and runtime expressions
like field paths.

For example, create a reusable operator to set fields dynamically:

```yaml
---
description: "Set a field to a value"
args:
  positional:
    - name: field
      type: field_path
    - name: value
      type: string
  named:
    - name: prefix
      type: string
      default: ""
---
$field = $prefix + $value
```

Use the operator with both constant and runtime arguments:

```tql
from {x: 1}
mypkg::set_field this.name, "Alice", prefix="User: "
```

```tql
{
  x: 1,
  name: "User: Alice",
}
```

Typed parameters now behave like lightweight contracts: the `type` key is
optional, but whenever it is provided the argument must be a constant expression
that can be evaluated at definition time. If the constant does not match the
declared type, the invocation fails with a diagnostic that points to the
mismatched argument. Field-path arguments (declared via `type: field_path`)
continue to accept selectors and cannot declare defaults.

Lastly, both the classic execution path and the `exec_with_ir` path share the
same argument parsing and type checking. User-defined operators therefore
produce identical results and diagnostics regardless of how the pipeline is
executed.
