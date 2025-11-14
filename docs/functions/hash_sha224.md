---
title: hash_sha224
category: Hashing
example: 'hash_sha224("foo")'
---

Computes a SHA-224 hash digest.

```tql
hash_sha224(x:any, [seed=string]) -> string
```

## Description

The `hash_sha224` function calculates a SHA-224 hash digest for the given value
`x`.

## Examples

### Compute a SHA-224 digest of a string

```tql
from {x: hash_sha224("foo")}
```

```tql
{x: "0808f64e60d58979fcb676c96ec938270dea42445aeefcd3a4e6f8db"}
```

## See Also

[`hash_md5`](/reference/functions/hash_md5),
[`hash_sha1`](/reference/functions/hash_sha1),
[`hash_sha256`](/reference/functions/hash_sha256),
[`hash_sha384`](/reference/functions/hash_sha384),
[`hash_sha512`](/reference/functions/hash_sha512),
[`hash_sha3_224`](/reference/functions/hash_sha3_224),
[`hash_sha3_256`](/reference/functions/hash_sha3_256),
[`hash_sha3_384`](/reference/functions/hash_sha3_384),
[`hash_sha3_512`](/reference/functions/hash_sha3_512),
[`hash_xxh3`](/reference/functions/hash_xxh3)
[`hmac_md5`](/reference/functions/hmac_md5),
[`hmac_sha1`](/reference/functions/hmac_sha1),
[`hmac_sha224`](/reference/functions/hmac_sha224),
[`hmac_sha256`](/reference/functions/hmac_sha256),
[`hmac_sha384`](/reference/functions/hmac_sha384),
[`hmac_sha512`](/reference/functions/hmac_sha512),
[`hmac_sha3_224`](/reference/functions/hmac_sha3_224),
[`hmac_sha3_256`](/reference/functions/hmac_sha3_256),
[`hmac_sha3_384`](/reference/functions/hmac_sha3_384),
[`hmac_sha3_512`](/reference/functions/hmac_sha3_512)
