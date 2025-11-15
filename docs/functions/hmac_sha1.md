---
title: hmac_sha1
category: Hashing
example: 'hmac_sha1("foo", secret("key_id"))'
---

Computes an HMAC-SHA-1 digest.

```tql
hmac_sha1(x:any, key:secret) -> string
```

## Description

The `hmac_sha1` function calculates an HMAC for `x` using the algorithm behind HMAC-SHA-1.

## Examples

### Compute an HMAC-SHA-1 digest of a string

```tql
from {x: hmac_sha1("foo", secret("key_id"))}
```

```tql
{x: "9baed91be7f58b57c824b60da7cb262b2ecafbd2"}
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
