---
sidebar_custom_props:
  operator:
    source: true
    transformation: true
    sink: true
---

# apply

Include the pipeline defined in another file.

## Synopsis

```
apply <file>
```

## Description

The `apply` operator searches for the given file, first in the current
directory, and then in `<config>/apply/` for every config directory, for example
`~/.config/tenzir/apply/`.

The `.tql` extension is automatically added to the filename, unless it already
has an extension.
