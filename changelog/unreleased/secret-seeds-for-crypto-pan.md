---
title: Secret seeds for Crypto-PAn
type: breaking
authors:
  - mavam
created: 2026-10-08T19:10:06.853392Z
---

The `seed` of `encrypt_cryptopan` and `decrypt_cryptopan` must now be a secret that holds exactly 32 bytes, instead of a string of hexadecimal digits:

```tql
let $seed = secret("cryptopan-key").decode_hex()
from {src_ip: 192.0.2.1}
src_ip = src_ip.encrypt_cryptopan(seed=$seed)
```

To keep your existing pseudonyms, store the same 64 hexadecimal digits in the secret store and decode them with `decode_hex`. Previously, the functions padded shorter seeds with zeros and truncated longer ones; pad or truncate such seeds to 64 digits before you store them. A seed of another size is now an error when the pipeline starts. Without a `seed`, the functions still use a key of zeros.
