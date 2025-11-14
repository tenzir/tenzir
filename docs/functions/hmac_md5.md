---
title: hmac_md5
category: Hashing
example: 'hmac_md5("foo", secret("key_id"))'
---

Computes an HMAC-MD5 digest.

```tql
hmac_md5(x:any, key:secret) -> string
```

## Description

The `hmac_md5` function calculates an HMAC for `x` using the algorithm behind HMAC-MD5.

## Examples

### Compute an HMAC-MD5 digest of a string

```tql
from {x: hmac_md5("foo", "secret")}
```

```tql
{x: "ba19fbc606a960051b60244e9a5ed3d2"}
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
