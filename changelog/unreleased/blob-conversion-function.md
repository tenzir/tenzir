---
title: Blob conversion function
type: feature
authors:
  - tobim
created: 2026-09-07T08:28:50.387363Z
---

The `blob` function converts UTF-8 strings to blobs without encoding the
payload first. Use it when you need to retain serialized data as a blob:

```tql
extra_data = blob(extra_data.print_ndjson())
```
