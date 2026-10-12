---
title: Encryption functions
type: feature
authors:
  - mavam
created: 2026-10-08T19:10:06.057292Z
---

New functions encrypt and decrypt values in pipelines, so that you can protect sensitive fields and still recover them later:

- `encrypt_aes_gcm_siv`, `encrypt_aes_gcm`, and `encrypt_aes_siv` provide authenticated encryption with AES. AES-GCM-SIV and AES-GCM draw a random nonce for every value, while AES-SIV encrypts deterministically, so that you can still group and join by the encrypted value.
- `encrypt_hpke` encrypts with a public key (RFC 9180), so that pipelines can protect data that only the holder of the private key can read.
- `encrypt_ff1` provides format-preserving encryption (NIST SP 800-38G). The output keeps the length and alphabet of the input, and characters outside the alphabet stay in place.

Every function has a `decrypt_*` counterpart. Keys must be secrets, except for the public key of `encrypt_hpke`. The optional `aad` and `tweak` arguments bind ciphertexts to their context:

```tql
let $key = secret("pii-key").decode_hex()
from {card: "4111-1111-1111-1111", email: "alice@example.com"}
card = card.encrypt_ff1(key=$key, tweak="card")
email = email.encrypt_aes_gcm_siv(key=$key)
```

Decryption returns `null` with a warning when the key or the associated data does not match, or when the ciphertext was modified.
