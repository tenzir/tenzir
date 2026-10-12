---
title: Secrets in function arguments
type: feature
authors:
  - mavam
  - IyeOnline
created: 2026-10-08T19:10:06.336188Z
---

Functions now accept secrets for sensitive arguments, such as the key of `hmac` and of the new encryption functions. Pass a `secret` call directly, or bind it with `let` to reuse it, including transformations such as `decode_hex`:

```tql
let $key = secret("hmac-key").decode_hex()
from {user: "alice"}
user = hmac(user, $key)
```

These arguments accept only secrets, so that keys never appear in pipeline definitions.
