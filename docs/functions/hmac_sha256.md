---
title: hmac_sha256
category: Hashing
example: 'hmac_sha256("foo", secret("key_id"))'
---

Computes an HMAC-SHA-256 digest.

```tql
hmac_sha256(x:any, key:secret) -> string
```

## Description

The `hmac_sha256` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA-256.

## Examples

### Compute an HMAC-SHA-256 digest of a string

```tql
from {x: hmac_sha256("foo", "secret")}
```

```tql
{x: "773ba44693c7553d6ee20f61ea5d2757a9a4f4a44d2841ae4e95b52e4cd62db4"}
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
