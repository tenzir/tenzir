---
title: Correct optimization of file contents
type: bugfix
authors:
  - mavam
created: 2026-10-13T10:23:44.598562Z
---

The optimizer no longer embeds the result of `file_contents()` in a pipeline plan or duplicates branch conditions that read files. This avoids treating contents that can change as a constant.

Calls to `file_contents()` are no longer supported in package constants. Pass the call directly to an operator argument instead, for example:

```tql
from_http "https://example.com", headers={"Authorization": file_contents("/run/token")}
```
