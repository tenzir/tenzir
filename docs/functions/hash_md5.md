---
title: hash_md5
category: Hashing
example: 'hash_md5("foo")'
---

Computes an MD5 hash digest.

```tql
hash_md5(x:any, [seed=string])
```

## Description

The `hash` function calculates a hash digest of a given value `x`.

### `x: any`

The value to hash.

### `seed = string (optional)`

The seed for the hash.

## Examples

### Compute an MD5 digest of a string

```tql
from { x: hash_md5("foo") }
```

```tql
{ x: "acbd18db4cc2f85cedef654fccc4a4d8" }
```

## See Also

[`hash_sha1`](/reference/functions/hash_sha1),
[`hash_sha224`](/reference/functions/hash_sha224),
[`hash_sha256`](/reference/functions/hash_sha256),
[`hash_sha512`](/reference/functions/hash_sha512),
[`hash_sha384`](/reference/functions/hash_sha384),
[`hash_sha3_224`](/reference/functions/hash_sha3_224),
[`hash_sha3_256`](/reference/functions/hash_sha3_256),
[`hash_sha3_384`](/reference/functions/hash_sha3_384),
[`hash_sha3_512`](/reference/functions/hash_sha3_512),
[`hash_xxh3`](/reference/functions/hash_xxh3),
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
