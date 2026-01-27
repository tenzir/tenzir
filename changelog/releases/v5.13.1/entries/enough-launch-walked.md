---
title: "`from_azure_blob_storage` operator"
type: feature
author: raxyte
created: 2025-08-26T13:30:48Z
pr: 5429
---

The new `from_azure_blob_storage` operator works similarly to `from_file` but
supports additional Azure Blob Storage specific options.

For example, you can set the `account_key`:
```tql
from_azure_blob_storage "abfs://container/data/*.csv", account_key="your-account-key"
```
