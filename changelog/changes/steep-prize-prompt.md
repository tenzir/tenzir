---
title: "Fix `context::create_geoip` without `db_path`"
type: bugfix
authors: dominiklohmann
pr: 5342
---

The `context::create_geoip` operator failed with a `message_mismatch` error when
no `db_path` option was provided. This was caused by an internal serialization
error, which we now fixed. This is the only known place where this error
occurred.
