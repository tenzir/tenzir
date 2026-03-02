This release adds the hmac function for computing Hash-based Message Authentication Codes over strings and blobs. It also fixes an assertion failure in array slicing that was introduced in v5.27.0.

## ✨ Features

### Add `hmac` function

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

_By @mavam and @codex in #5846._

## 🐞 Bug fixes

### Fixed an assertion failure in slicing

We fixed a bug that would cause an assertion failure _"Index error: array slice would exceed array length"_. This was introduced as part of an optimization in Tenzir Node v5.27.0.

_By @IyeOnline in #5842._
