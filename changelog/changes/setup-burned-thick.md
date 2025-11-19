---
title: "Improved Encoding and Decoding Support"
type: feature
authors: IyeOnline
pr: 5572
---

We have added two new functions: `encode_base58` and `decode_base58`. As the names
imply, these Base58 encode/decode `string` or `blob` values, just like the already
existing functions for Base64, Hex or URL encoding.

With this change, we also enabled the usage of all encoding and decoding functions
on `secret` values.
