---
title: Luhn check digits and CRC-32 checksums
type: feature
authors:
  - mavam
created: 2026-10-09T08:27:01.206556Z
---

New functions validate and compute Luhn check digits, so that card numbers stay valid after format-preserving encryption:

- `is_luhn_valid` tests whether a string of digits has a valid Luhn checksum. It returns `false` for empty strings and for strings with other characters, such as spaces or dashes. A valid checksum is necessary, but not sufficient, for a valid card number.
- `luhn_check_digit` returns the digit that makes a string of digits Luhn-valid when appended.
- `hash_crc32` computes the CRC-32 checksum of a value. CRC-32 is not cryptographic, so use `hash_sha256` or `hmac` to pseudonymize data.

`encrypt_ff1` keeps the length and digits of a card number, but not its check digit. Encrypt the number without its check digit and append a new one to get a valid card number:

```tql
let $key = secret("pii-key").decode_hex()
from {card: "4111111111111111"}
// Only touch card numbers: 12 to 19 digits with a valid check digit.
if card.is_luhn_valid() and card.length_bytes() >= 12 and card.length_bytes() <= 19 {
  // Encrypt all digits except for the check digit at the end.
  card = card.slice(end=-1).encrypt_ff1(key=$key, tweak="card")
  // Append the check digit that matches the encrypted digits.
  card = card + card.luhn_check_digit().string()
}
```

```tql
{
  card: "9684338133581379",
}
```

Decryption works the same way with `decrypt_ff1`. Keep the guard: the round trip recomputes the last digit, so it restores only valid card numbers, and the length check leaves values alone that have too few or too many digits for a card number.
