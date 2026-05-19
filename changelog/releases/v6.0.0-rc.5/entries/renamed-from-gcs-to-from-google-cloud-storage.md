---
title: Renamed `from_gcs` to `from_google_cloud_storage`
type: breaking
author: raxyte
pr: 5766
created: 2026-04-30T13:03:17.226108Z
---

The `from_gcs` operator has been renamed to `from_google_cloud_storage` so
that its name matches the new `to_google_cloud_storage` writer:

```tql
// Before:
from_gcs "gs://my-bucket/data/**.json"

// After:
from_google_cloud_storage "gs://my-bucket/data/**.json"
```

Update test suites that reference `from_gcs` in `requires.operators`
accordingly.
