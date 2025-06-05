---
title: hash_sha1
category: Hashing
example: 'hash_sha1("foo")'
---

Computes a SHA-1 hash digest.

```tql
hash_sha1(x:any, [seed=string]) -> string
```

## Description

The `hash_sha1` function calculates a SHA-1 hash digest for the given value `x`.

## Examples

### Compute a SHA-1 digest of a string

```tql
from {x: hash_sha1("foo")}
```

```tql
{x: "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"}
```

## See Also

[`hash_md5`](/reference/functions/hash_md5),
[`hash_sha224`](/reference/functions/hash_sha224),
[`hash_sha256`](/reference/functions/hash_sha256),
[`hash_sha384`](/reference/functions/hash_sha384),
[`hash_sha512`](/reference/functions/hash_sha512),
[`hash_xxh3`](/reference/functions/hash_xxh3)
