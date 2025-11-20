---
title: hash_sha512
category: Hashing
example: 'hash_sha512("foo")'
---

Computes a SHA-512 hash digest.

```tql
hash_sha512(x:any, [seed=string]) -> string
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

[`hash_md5`](/reference/functions/hash_md5),
[`hash_sha1`](/reference/functions/hash_sha1),
[`hash_sha224`](/reference/functions/hash_sha224),
[`hash_sha256`](/reference/functions/hash_sha256),
[`hash_sha384`](/reference/functions/hash_sha384),
[`hash_sha3_224`](/reference/functions/hash_sha3_224),
[`hash_sha3_256`](/reference/functions/hash_sha3_256),
[`hash_sha3_384`](/reference/functions/hash_sha3_384),
[`hash_sha3_512`](/reference/functions/hash_sha3_512),
[`hash_xxh3`](/reference/functions/hash_xxh3)
