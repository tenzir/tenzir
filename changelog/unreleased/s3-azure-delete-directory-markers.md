---
title: "No more directory markers for S3 and Azure"
type: bugfix
author: jachris
pr: 5669
---

Deleting files from S3 or Azure Blob Storage via `load_file` or `from_file` with
the `delete` option no longer creates empty directory marker objects in the
parent directory.
