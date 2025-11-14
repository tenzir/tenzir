---
title: "OpenSSL hash backend and SHA3/HMAC functions"
type: feature
authors: mavam
pr: 5574
---

All `hash_*` functions (MD5/SHA1/SHA2) now use OpenSSL's EVP implementation
instead of the previous digestpp copies. We've also added SHA3-224/256/384/512
variants to the `hash_*` family and introduced `hmac_*` functions that take a
`secret` key argument.

## Example: SHA3 hash

```tql
from {x: hash_sha3_256("foo")}
```

```tql
{x: "76d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01"}
```

## Example: HMAC

```tql
from {x: hmac_sha256("foo", "secret")}
```

```tql
{x: "773ba44693c7553d6ee20f61ea5d2757a9a4f4a44d2841ae4e95b52e4cd62db4"}
```
