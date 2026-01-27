---
title: "Account key authentication for Azure Blob Storage"
type: feature
author: raxyte
created: 2025-07-30T10:04:50Z
pr: 5380
---

The `load_azure_blob_storage` and `save_azure_blob_storage` operators now
support account key (shared key) authentication via a new `account_key` option.
This provides an additional method for accessing Azure Blob Storage, alongside
existing authentication options.
