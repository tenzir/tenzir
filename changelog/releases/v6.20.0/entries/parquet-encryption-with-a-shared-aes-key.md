---
title: Parquet encryption with a shared AES key
type: feature
authors:
  - zedoraps
created: 2026-10-20T12:12:14.460867Z
---

You can now encrypt Parquet files before storing them on external object storage
and decrypt them when reading them back:

```tql
from {message: "sensitive event"}
write_parquet encryption_key=decode_base64(secret("PARQUET_KEY"))
read_parquet encryption_key=decode_base64(secret("PARQUET_KEY"))
```

The `encryption_key` argument takes a raw 16-, 24-, or 32-byte AES key. The
example explicitly decodes a base64-encoded secret. Secrets or binary key files
that already contain raw key bytes need no decoding. All columns and the footer
use the same key with `AES_GCM_V1` encryption and authentication. Reading
requires the same key. A reader configured with `encryption_key` rejects
plaintext files.
