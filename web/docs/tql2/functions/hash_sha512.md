# hash_sha512

Computes a SHA-512 hash digest.

```tql
hash_sha512(x:any, [seed=str]) -> string
```

## Description

The `hash_sha512` function calculates a SHA-512 hash digest for the given value
`x`.

## Examples

### Compute a SHA-512 digest of a string

```tql
from {x: hash_sha512("foo")}
```

```tql
{x: "f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda19594a7eb539453e1ed7"}
```

## See Also

[`hash_md5`](hash_md5.md), [`hash_sha1`](hash_sha1.md),
[`hash_sha224`](hash_sha224.md), [`hash_sha256`](hash_sha256.md),
[`hash_sha384`](hash_sha384.md), [`hash_xxh3`](hash_xxh3.md)
