---
title: "Add `encode_url` and `decode_url` functions"
type: feature
authors: dominiklohmann
pr: 5168
---

The `encode_url` and `decode_url` functions encode and decode URLs. For example,
`"Hello%20World".decode_url()` returns `b"Hello World"`.
