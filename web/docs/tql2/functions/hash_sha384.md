# hash_sha384

Computes a SHA-384 hash digest.

```tql
hash_sha384(x:any, [seed=str]) -> string
```

## Description

The `hash_sha384` function calculates a SHA-384 hash digest for the given value
`x`.

## Examples

### Compute a SHA-384 digest of a string

```tql
from {x: hash_sha384("foo")}
```

```tql
{x: "98c11ffdfdd540676b1a137cb1a22b2a70350c9a44171d6b1180c6be5cbb2ee3f79d532c8a1dd9ef2e8e08e752a3babb"}
```

## See Also

[`hash_md5`](hash_md5.md), [`hash_sha1`](hash_sha1.md),
[`hash_sha224`](hash_sha224.md), [`hash_sha256`](hash_sha256.md),
[`hash_sha512`](hash_sha512.md), [`hash_xxh3`](hash_xxh3.md)
