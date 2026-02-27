---
title: "Add `hmac` function"
type: feature
author: [mavam, codex]
created: 2026-02-27T00:00:00Z
pr: 5846
---

The new experimental `hmac` function computes Hash-based Message
Authentication Codes (HMAC) for strings and blobs. It supports SHA-256
(default), SHA-512, SHA-384, SHA-1, and MD5 algorithms.

Note: The `key` parameter is currently a plain string because function
arguments cannot be secrets yet. We plan to change this in the future.

```tql
from {
  signature: hmac("hello world", "my-secret-key"),
}
```

```tql
{
  signature: "90eb182d8396f16d4341d582047f45c0a97d73388c5377d9ced478a2212295ad",
}
```

Specify a different algorithm with the `algorithm` parameter:

```tql
from {
  signature: hmac("hello world", "my-secret-key", algorithm="sha512"),
}
```
