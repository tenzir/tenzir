---
title: "User-defined operators support arguments and options"
type: feature
authors: tobim
---

User-defined operators in packages can now declare arguments and options in their
YAML frontmatter, enabling parameterized operator definitions with the same calling
convention as built-in operators.

Arguments are positional parameters, while options are named parameters with optional
default values. Both support constant values and runtime expressions like field paths.

For example, create a reusable operator to set fields dynamically:

```yaml
---
description: "Set a field to a value"
args:
  - name: field
    type: field_path
  - name: value
    type: string
options:
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
