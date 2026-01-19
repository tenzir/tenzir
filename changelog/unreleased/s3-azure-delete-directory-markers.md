---
title: "No more directory markers for S3 and Azure"
type: bugfix
author: jachris
pr: 5669
---

Deleting files from S3 or Azure Blob Storage via `from_s3` or
`from_azure_blob_storage` with the `remove=true` option no longer creates empty
directory marker objects in the parent directory when the last file of the
directory is deleted.
