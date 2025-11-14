---
title: "OpenSSL hash backend and SHA3 functions"
type: feature
authors: mavam
pr: 5574
---

All `hash_*` functions (MD5/SHA1/SHA2) now use OpenSSL's EVP implementation
instead of the previous digestpp copies.

We've also added SHA3-224/256/384/512 to `hash_*` function family. For example,
compute a SHA3-256 hash digest as follows:

```tql
from {x: hash_sha3_256("foo")}
```

```tql
{x: "76d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01"}
```
