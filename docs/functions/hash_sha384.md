---
title: hash_sha384
category: Hashing
example: 'hash_sha384("foo")'
---

Computes a SHA-384 hash digest.

```tql
hash_sha384(x:any, [seed=string]) -> string
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

[`hash_md5`](/reference/functions/hash_md5),
[`hash_sha1`](/reference/functions/hash_sha1),
[`hash_sha224`](/reference/functions/hash_sha224),
[`hash_sha256`](/reference/functions/hash_sha256),
[`hash_sha512`](/reference/functions/hash_sha512),
[`hash_sha3_224`](/reference/functions/hash_sha3_224),
[`hash_sha3_256`](/reference/functions/hash_sha3_256),
[`hash_sha3_384`](/reference/functions/hash_sha3_384),
[`hash_sha3_512`](/reference/functions/hash_sha3_512),
[`hash_xxh3`](/reference/functions/hash_xxh3)
