---
title: '`$file` let-binding for filesystem readers'
type: breaking
author: raxyte
pr: 6001
created: 2026-04-30T13:00:47.360929Z
---

The filesystem and cloud object reader operators (`from_file`, `from_s3`,
`from_azure_blob_storage`, `from_google_cloud_storage`) no longer accept the
`path_field` option. Instead, the parsing subpipeline now has access to a
`$file` let-binding describing the source file:

| Field   | Type     | Description                              |
| :------ | :------- | :--------------------------------------- |
| `path`  | `string` | The absolute path of the file being read |
| `mtime` | `time`   | The last modification time of the file   |

To attach the source path to each event:

```tql
// Before:
from_file "/data/*.json", path_field=source

// After:
from_file "/data/*.json" {
  read_json
  source = $file.path
}
```

This makes per-file metadata available throughout the parsing subpipeline
rather than only on emitted events.
