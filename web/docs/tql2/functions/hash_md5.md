# hash_md5

Computes an MD5 hash digest.

## Synopsis

```tql
hash_md5(x:any, [seed=str])
```

## Description

The `hash` function calculates a hash digest of a given value `x`.

### `x: any`

The value to hash.

### `seed = str (optional)`

The seed for the hash.

## Examples

### Compute an MD5 digest of a string

```tql
from { x: hash_md5("foo") }
```

```tql
{ x: "acbd18db4cc2f85cedef654fccc4a4d8" }
```

## See Also

[hash_sha1](hash_sha1.md), [hash_sha224](hash_sha224.md),
[hash_sha256](hash_sha256.md), [hash_sha384](hash_sha384.md),
[hash_sha512](hash_sha512.md), [hash_xxh3](hash_xxh3.md)
