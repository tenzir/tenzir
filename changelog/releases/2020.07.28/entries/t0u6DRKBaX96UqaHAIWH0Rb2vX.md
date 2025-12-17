---
title: "Add MsgPack-based Table Slice implementation"
type: feature
author: dominiklohmann
created: 2020-07-13T13:12:38Z
pr: 975
---

We open-sourced our [MessagePack](http://msgpack.org)-based table slice
implementation, which provides a compact row-oriented encoding of data. This
encoding works well for binary formats (e.g., PCAP) and access patterns that
involve materializing entire rows. The MessagePack table slice is the new
default when Apache Arrow is unavailable. To enable parsing into MessagePack,
you can pass `--table-slice-type=msgpack` to the `import` command, or set the
configuration option `import.table-slice-type` to `'msgpack'`.
