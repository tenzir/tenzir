---
title: hash_sha3_512
category: Hashing
example: 'hash_sha3_512("foo")'
---

Computes a SHA3-512 hash digest.

```tql
hash_sha3_512(x:any, [seed=string]) -> string
```

## Description

The `hash_sha3_512` function calculates a SHA3-512 hash digest for the given
value `x`.

## Examples

### Compute a SHA3-512 digest of a string

```tql
from {x: hash_sha3_512("foo")}
```

```tql
{x: "4bca2b137edc580fe50a88983ef860ebaca36c857b1f492839d6d7392452a63c82cbebc68e3b70a2a1480b4bb5d437a7cba6ecf9d89f9ff3ccd14cd6146ea7e7"}
```

## See Also

[`hash_md5`](/reference/functions/hash_md5),
[`hash_sha1`](/reference/functions/hash_sha1),
[`hash_sha224`](/reference/functions/hash_sha224),
[`hash_sha256`](/reference/functions/hash_sha256),
[`hash_sha384`](/reference/functions/hash_sha384),
[`hash_sha512`](/reference/functions/hash_sha512),
[`hash_sha3_224`](/reference/functions/hash_sha3_224),
[`hash_sha3_256`](/reference/functions/hash_sha3_256),
[`hash_sha3_384`](/reference/functions/hash_sha3_384),
[`hash_xxh3`](/reference/functions/hash_xxh3)
