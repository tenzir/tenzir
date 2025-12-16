---
title: "Filter files by modification times with `max_age`"
type: feature
authors: raxyte
pr: 5611
---

The `from_file`, `from_s3`, `from_gcs`, and `from_azure_blob_storage` operators
now support an optional `max_age` parameter that filters files based on their
last modification time. Only files modified within the specified duration from
now will be processed.

**Example**

Process only files modified in the last hour:

```tql
from_file "/var/log/security/*.json", max_age=1h
```
