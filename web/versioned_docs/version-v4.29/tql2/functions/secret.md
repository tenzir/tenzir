# secret

Reads a secret from a store.

```tql
secret(x:string) -> string
```

## Description

The `secret` function retrieves the value associated with the key `x` and
replaces it with the built-in secret store, which is a section in the
`tenzir.yaml` configuration file:

```yaml
tenzir:
  secrets:
    # Add your secrets there.
    geheim: 1528F9F3-FAFA-45B4-BC3C-B755D0E0D9C2
```

## Examples

### Read a secret from the configuration

```tql
from {x: secret("geheim")}
```

```tql
{x: "1528F9F3-FAFA-45B4-BC3C-B755D0E0D9C2"}
```
