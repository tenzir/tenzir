---
title: Dedicated FTP source and sink operators
type: breaking
author: mavam
pr: 6044
created: 2026-04-30T12:58:47.461872Z
---

Two new operators provide first-class FTP and FTPS support with parsing and
printing subpipelines:

- `from_ftp` downloads bytes from an FTP or FTPS server and forwards them to
  the parsing subpipeline.
- `to_ftp` uploads bytes produced by the printing subpipeline to an FTP or
  FTPS server.

```tql
from_ftp "ftp://user:pass@ftp.example.org/path/to/file.ndjson" {
  read_ndjson
}
```

```tql
to_ftp "ftp://user:pass@ftp.example.org/a/b/c/events.ndjson" {
  write_ndjson
}
```

The `load_ftp` and `save_ftp` operators have been removed, and the `ftp://`
and `ftps://` URL schemes no longer dispatch via `from` and `to`. Use
`from_ftp` and `to_ftp` directly.
