---
title: Reliable loading of large context states
type: bugfix
authors:
  - tobim
created: 2026-09-04T13:49:24.577937Z
---

`context_load` now correctly reassembles context state that arrives in multiple
chunks. Previously, loading could discard all but the final chunk of the input,
which made loading larger context states—such as a DCSO Bloom filter fed through
`from_file` and `decompress_gzip`—fail with errors like `invalid version` or
`bloom filter buffer too small`, depending on how the byte stream happened to be
chunked. The same file could load on one machine and fail on another.
