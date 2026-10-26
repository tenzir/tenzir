---
title: Secret seeds for Crypto-PAn
type: change
authors:
  - mavam
created: 2026-10-08T19:10:06.853392Z
---

The `seed` of `encrypt_cryptopan` and `decrypt_cryptopan` must now hold exactly 32 bytes, instead of being a string of hexadecimal digits. Read it as a secret, so that it stays out of the pipeline definition:

```tql
let $seed = secret("cryptopan-key").decode_hex()
from {src_ip: 192.0.2.1}
src_ip = src_ip.encrypt_cryptopan(seed=$seed)
```

To keep your existing pseudonyms, store the same 64 hexadecimal digits in the secret store and decode them with `decode_hex`. Previously, the functions padded shorter seeds with zeros and truncated longer ones; pad or truncate such seeds to 64 digits before you store them. A seed of another size is now an error when the pipeline starts. A plain blob, such as `decode_hex("…")`, also works as a seed, but then the seed appears in the pipeline definition. Without a `seed`, the functions still use a key of zeros.
