---
title: hmac_sha384
category: Hashing
example: 'hmac_sha384("foo", secret("key_id"))'
---

Computes an HMAC-SHA-384 digest.

```tql
hmac_sha384(x:any, key:secret) -> string
```

## Description

The `hmac_sha384` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA-384.

## Examples

### Compute an HMAC-SHA-384 digest of a string

```tql
from {x: hmac_sha384("foo", secret("key_id"))}
```

```tql
{x: "0edb7068ecbf4de2c47b8819fd534333379f208f989c51018d03ee1155e4c0740a418ec220d4260eabcb2d090b16de6e"}
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
