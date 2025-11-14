---
title: hmac_sha3_512
category: Hashing
example: 'hmac_sha3_512("foo", secret("key_id"))'
---

Computes an HMAC-SHA3-512 digest.

```tql
hmac_sha3_512(x:any, key:secret) -> string
```

## Description

The `hmac_sha3_512` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA3-512.

## Examples

### Compute an HMAC-SHA3-512 digest of a string

```tql
from {x: hmac_sha3_512("foo", secret("key_id"))}
```

```tql
{x: "eb1ffb70a19f2dd32ca4eabc5dcc0ff9a022821cc9bf18ab9c00f0589eaa45929772e25b05ea7dc6ac75c848a50de94d8f75d60c8d7671826934231d0b304ad9"}
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
