# hash_xxh3

Computes an XXH3 hash digest.

```tql
hash_xxh3(x:any, [seed=str]) -> string
```

## Description

The `hash_xxh3` function calculates a 64-bit XXH3 hash digest for the given
value `x`.

## Examples

### Compute an XXH3 digest of a string

```tql
from {x: hash_xxh3("foo")}
```

```tql
{x: "ab6e5f64077e7d8a"}
```

## See Also

[`hash_md5`](hash_md5.md), [`hash_sha1`](hash_sha1.md),
[`hash_sha224`](hash_sha224.md), [`hash_sha256`](hash_sha256.md),
[`hash_sha384`](hash_sha384.md), [`hash_sha512`](hash_sha512.md)
