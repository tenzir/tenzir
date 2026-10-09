---
title: Secrets are censored on assignment
type: change
authors:
  - IyeOnline
created: 2026-10-08T07:51:36.000000Z
---

Assigning a secret to a field with `set` or `select` now stores `"***"` instead and emits a warning. This includes secrets nested in records or lists. `lag` and `context::enrich` with `format="ocsf"` censor secrets the same way. Secrets remain usable within expressions, for example `digest = hmac(value, secret("key"))`, but can no longer travel with events.
