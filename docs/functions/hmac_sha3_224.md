---
title: hmac_sha3_224
category: Hashing
example: 'hmac_sha3_224("foo", secret("key_id"))'
---

Computes an HMAC-SHA3-224 digest.

```tql
hmac_sha3_224(x:any, key:secret) -> string
```

## Description

The `hmac_sha3_224` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA3-224.

## Examples

### Compute an HMAC-SHA3-224 digest of a string

```tql
from {x: hmac_sha3_224("foo", "secret")}
```

```tql
{x: "fe221c32c42a0d836ae9a4de90ce50d80d366fec10615d9f6fe99967"}
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
[`hash_sha3_512`](/reference/functions/hash_sha3_512)
