---
title: Secret keys for hmac
type: feature
authors:
  - mavam
created: 2026-10-08T19:10:06.576444Z
---

The key of `hmac` can now be a secret, so that it stays out of pipeline definitions:

```tql
from {user: "alice"}
user = hmac(user, secret("hmac-key"))
```

Transformations of a secret, such as `secret("hmac-key").decode_hex()`, still count as secrets. Plain string and blob keys continue to work.
