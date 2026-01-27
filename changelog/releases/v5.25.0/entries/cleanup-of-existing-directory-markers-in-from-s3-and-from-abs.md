---
title: Cleanup of existing directory markers in from_s3 and from_abs
type: change
authors:
  - jachris
pr: 5670
created: 2026-01-19T12:54:24.634128Z
---

The `from_s3` and `from_azure_blob_storage` operators now also delete existing
directory marker objects along the glob path when `remove=true`. Directory
markers are zero-byte objects with keys ending in `/` that some cloud storage
tools create. These artifacts can accumulate over time, increasing API costs and
slowing down listing operations.
