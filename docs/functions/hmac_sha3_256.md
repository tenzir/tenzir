---
title: hmac_sha3_256
category: Hashing
example: 'hmac_sha3_256("foo", secret("key_id"))'
---

Computes an HMAC-SHA3-256 digest.

```tql
hmac_sha3_256(x:any, key:secret) -> string
```

## Description

The `hmac_sha3_256` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA3-256.

## Examples

### Compute an HMAC-SHA3-256 digest of a string

```tql
from {x: hmac_sha3_256("foo", "secret")}
```

```tql
{x: "9a536b6e6619c2b98c82d31bc45bdf5811c66d69b1ab29486b0d41324f01940b"}
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
