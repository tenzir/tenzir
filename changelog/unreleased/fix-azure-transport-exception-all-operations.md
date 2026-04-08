---
title: Fix crash on Azure SSL/transport errors during read and write operations
type: bugfix
authors:
  - claude
created: 2026-04-08T00:00:00.000000Z
---

The previous fix for Azure `TransportException` crashes (v5.30.0) only
covered blob deletion operations. The same unhandled exception could still
crash the node during file listing, reading, or writing via
`load_azure_blob_storage`, `save_azure_blob_storage`, and
`from_azure_blob_storage`. All Azure Blob Storage operations now catch
transport-level exceptions (e.g., SSL certificate errors) gracefully.
