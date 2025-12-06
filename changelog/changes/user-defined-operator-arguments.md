---
title: "Argument support for User-defined operators"
type: feature
authors: tobim
---

User-defined operators in packages can now declare arguments in their YAML
frontmatter, enabling parameterized operator definitions with the same calling
convention as built-in operators.

Arguments can be positional or named. Both support optional default values and
can be called with literals, constant expressions, or dynamically evaluated
runtime expressions such as fields.

For example, create a reusable operator to set fields dynamically:

```yaml
---
description: "Set a field to a value"
args:
  positional:
    - name: field
      type: field
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

Parameters can be typed with a type name passed in the namesake field. In case
the passed in expression can be evaluated at instantiation time it is checked
against the type and a diagnostic is returned if it does not match. In case a
type check is not possible because the expression contains references to
run-time data, the type check is omitted, and potential errors will be flagged at
runtime. Field-path arguments (declared via `type: field`) accept selectors and
cannot declare defaults.
