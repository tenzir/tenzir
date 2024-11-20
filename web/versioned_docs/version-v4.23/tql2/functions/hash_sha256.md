# hash_sha256

Computes a SHA-256 hash digest.

```tql
hash_sha256(x:any, [seed=str]) -> string
```

## Description

The `hash_sha256` function calculates a SHA-256 hash digest for the given value
`x`.

## Examples

### Compute a SHA-256 digest of a string

```tql
from {x: hash_sha256("foo")}
```

```tql
{x: "2c26b46b68ffc68ff99b453c1d30413413422e6e"}
```

## See Also

[`hash_md5`](hash_md5.md), [`hash_sha1`](hash_sha1.md),
[`hash_sha224`](hash_sha224.md), [`hash_sha384`](hash_sha384.md),
[`hash_sha512`](hash_sha512.md), [`hash_xxh3`](hash_xxh3.md)
