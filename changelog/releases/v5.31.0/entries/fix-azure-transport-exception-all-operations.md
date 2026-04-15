---
title: Fix crash on Azure SSL/transport errors during read and write operations
type: bugfix
authors:
  - claude
created: 2026-04-08T00:00:00.000000Z
---

Bumped Apache Arrow from 23.0.0 to 23.0.1, which includes an upstream fix
for unhandled `Azure::Core::Http::TransportException` in Arrow's
`AzureFileSystem` methods. Previously, transport-level errors (e.g., SSL
certificate failures) could crash the node during file listing, reading, or
writing. Additionally, the direct Azure SDK calls in the blob deletion code
paths now catch `Azure::Core::RequestFailedException` (the common base of
both `StorageException` and `TransportException`) instead of listing
specific exception types.
