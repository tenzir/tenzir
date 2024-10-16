# xxhash3

Computes a hash digest of a given field.

## Synopsis

```tql
xxhash3(<value>, [seed=str])
```

## Description

The `hash` operator calculates a hash digest of a given field.

### `<value>`

The value to hash.

### `seed = str (optional)`

A seed value for the hash.

## Examples

Hash all values of the field `username` using the seed `"xxx"` and store
the digest in a new field `username_hashed`:

```tql
from { username: "Tenzir" }
username_hashed = xxhash3(username, seed="xxx")
write_json
```
