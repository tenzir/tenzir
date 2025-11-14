---
title: hmac_sha512
category: Hashing
example: 'hmac_sha512("foo", secret("key_id"))'
---

Computes an HMAC-SHA-512 digest.

```tql
hmac_sha512(x:any, key:secret) -> string
```

## Description

The `hmac_sha512` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA-512.

## Examples

### Compute an HMAC-SHA-512 digest of a string

```tql
from {x: hmac_sha512("foo", secret("key_id"))}
```

```tql
{x: "82df7103de8d82de45e01c45fe642b5d13c6c2b47decafebc009431c665c6fa5f3d1af4e978ea1bde91426622073ebeac61a3461efd467e0971c788bc8ebdbbe"}
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
