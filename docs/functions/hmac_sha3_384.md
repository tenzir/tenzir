---
title: hmac_sha3_384
category: Hashing
example: 'hmac_sha3_384("foo", secret("key_id"))'
---

Computes an HMAC-SHA3-384 digest.

```tql
hmac_sha3_384(x:any, key:secret) -> string
```

## Description

The `hmac_sha3_384` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA3-384.

## Examples

### Compute an HMAC-SHA3-384 digest of a string

```tql
from {x: hmac_sha3_384("foo", secret("key_id"))}
```

```tql
{x: "6e9ee49024a67173753a966e94378868ed566f508932a704a366fe68d2edac378036febab070b27c0b802063142756b8"}
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
