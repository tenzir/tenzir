---
title: "Enhanced file renaming in `from_file` operator"
type: feature
author: dominiklohmann
created: 2025-06-30T21:33:06Z
pr: 5303
---

The `from_file` operator now provides enhanced file renaming capabilities when
using the `rename` parameter. These improvements make file operations more
robust and user-friendly.

**Directory creation**: The operator now automatically creates intermediate
directories when renaming files to paths that don't exist yet. For example, if
you rename a file to `/new/deep/directory/structure/file.txt`, all necessary
parent directories (`/new`, `/new/deep`, `/new/deep/directory`,
`/new/deep/directory/structure`) will be created automatically.

```tql
from_file "/data/*.json", rename=path => f"/processed/by-date/2024/01/{path.file_name()}"
```

**Trailing slash handling**: When the rename target ends with a trailing slash,
the operator now automatically appends the original filename. This makes it easy
to move files to different directories while preserving their names.

```tql
// This will rename "/input/data.json" to "/output/data.json"
from_file "/input/*.json", rename=path => "/output/"
```

Previously, you would have needed to manually extract and append the filename:

```tql
// Old approach - no longer necessary
from_file "/input/*.json", rename=path => f"/output/{path.file_name()}"
```
