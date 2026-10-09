---
title: Secret keys for hmac
type: change
authors:
  - mavam
created: 2026-10-08T19:10:06.576444Z
---

The key of `hmac` must now be a secret, so that it stays out of pipeline definitions. Read the key from a secret store instead of passing a string:

```tql
from {user: "alice"}
user = hmac(user, secret("hmac-key"))
```

A plain string key is an error when the pipeline starts. Transformations of a secret, such as `secret("hmac-key").decode_hex()`, still count as secrets.
