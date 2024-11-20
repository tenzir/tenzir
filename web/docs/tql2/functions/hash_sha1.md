# hash_sha1

Computes a SHA-1 hash digest.

```tql
hash_sha1(x:any, [seed=str]) -> string
```

## Description

The `hash_sha1` function calculates a SHA-1 hash digest for the given value `x`.

## Examples

### Compute a SHA-1 digest of a string

```tql
from {x: hash_sha1("foo")}
```

```tql
{x: "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"}
```

## See Also

[`hash_md5`](hash_md5.md), [`hash_sha224`](hash_sha224.md),
[`hash_sha256`](hash_sha256.md), [`hash_sha384`](hash_sha384.md),
[`hash_sha512`](hash_sha512.md), [`hash_xxh3`](hash_xxh3.md)
