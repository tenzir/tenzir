---
title: New data model
type: feature
authors:
  - IyeOnline
created: 2026-09-24T00:00:00Z
---

Tenzir now uses a new data model. It brings:

- Faster pipelines through better batching.
- Lists that contain values of different types.
- Secrets as function arguments.

Read more about it in our [blog](https://tenzir.com/blog).

Some computed values differ between the two data models:

- The `type_id` function and the `schema_id` that `measure` reports return
  different IDs for the same type.
- Hash functions such as `hash_sha256` and `hash_xxh3` return different digests
  for records and lists. Hashes of other values do not change.

If you store such values, for example to deduplicate events or findings, values
computed before and after the upgrade no longer match.

To switch back to the old data model, set `tenzir.nova: false` in your `tenzir.yaml` or the environment variable `TENZIR_NOVA=false`. The old data model is deprecated and will be removed in a future release.
