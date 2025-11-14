---
title: hash_sha3_256
category: Hashing
example: 'hash_sha3_256("foo")'
---

Computes a SHA3-256 hash digest.

```tql
hash_sha3_256(x:any, [seed=string]) -> string
```

## Description

The `hash_sha3_256` function calculates a SHA3-256 hash digest for the given
value `x`.

## Examples

### Compute a SHA3-256 digest of a string

```tql
from {x: hash_sha3_256("foo")}
```

```tql
{x: "76d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01"}
```

## See Also

[`hash_md5`](/reference/functions/hash_md5),
[`hash_sha1`](/reference/functions/hash_sha1),
[`hash_sha224`](/reference/functions/hash_sha224),
[`hash_sha256`](/reference/functions/hash_sha256),
[`hash_sha384`](/reference/functions/hash_sha384),
[`hash_sha512`](/reference/functions/hash_sha512),
[`hash_sha3_224`](/reference/functions/hash_sha3_224),
[`hash_sha3_384`](/reference/functions/hash_sha3_384),
[`hash_sha3_512`](/reference/functions/hash_sha3_512),
[`hash_xxh3`](/reference/functions/hash_xxh3)
