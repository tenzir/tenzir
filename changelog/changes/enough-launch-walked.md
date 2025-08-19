---
title: "`from_azure_blob_storage` operator"
type: feature
authors: raxyte
pr: 5429
---

The new `from_azure_blob_storage` operator works similarly to `from_file` but
supports additional Azure Blob Storage specific options.

For example, you can set the `account_key`:
```tql
from_azure_blob_storage "abfs://container/data/*.csv", account_key="your-account-key"
```
