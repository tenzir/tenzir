---
title: Crypto-PAn decryption
type: feature
authors:
  - mavam
  - codex
created: 2026-06-06T00:00:00Z
pr: 6259
---

The new `decrypt_cryptopan` function decrypts IP addresses that were encrypted
with `encrypt_cryptopan` and the same seed. The function can also force the
Crypto-PAn domain with `family="ipv4"` or `family="ipv6"` when decrypting an
ambiguous ciphertext.
