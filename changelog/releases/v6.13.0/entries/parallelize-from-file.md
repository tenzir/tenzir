---
title: Parallelize file-reading operators
type: feature
authors:
  - aljazerzen
created: 2026-08-18T00:00:00.000000Z
---

`from_file`, `from_s3`, `from_google_cloud_storage`, and
`from_azure_blob_storage` now run as multiple instances when the pipeline is
parallelized. The instances split the discovered files among themselves, so a
directory with many files is read on several cores instead of one:

```tql
from_file "/var/log/**/*.json" {
  read_json
}
```

Which instance reads a file depends only on its path. Every file is therefore
read, removed, and renamed exactly once, both when rescanning in `watch` mode
and after a restart. Each instance reads its files one after another, so the
degree of parallelism determines how many files Tenzir reads at the same time.
