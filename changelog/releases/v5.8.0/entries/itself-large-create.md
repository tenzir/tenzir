---
title: "HTTP format and compression inference"
type: feature
author: raxyte
created: 2025-07-02T13:22:08Z
pr: 5300
---

The `from_http` and `http` operators now automatically infer the file format
(such as JSON, CSV, Parquet, etc.) and compression type (such as gzip, zstd,
etc.) directly from the URL's file extension, just like the generic `from`
operator. This makes it easier to load data from HTTP sources without manually
specifying the format or decompression step.

If the format or compression cannot be determined from the URL, the operators
will fall back to using the HTTP `Content-Type` and `Content-Encoding` response
headers to determine how to parse and decompress the data.

**Examples**

**Inference Succeeds**

```tql
from_http "https://example.org/data/events.csv.zst"
```

The operator infers both the `zstd` compression and the `CSV` format from the
file extension, decompresses, and parses accordingly.

**Inference Fails, Fallback to Headers**

```tql
from_http "https://example.org/download"
```

If the URL does not contain a recognizable file extension, the operator will use
the HTTP `Content-Type` and `Content-Encoding` headers from the response to
determine the format and compression.

**Manual Specification Required**

```tql
from_http "https://example.org/archive" {
  decompress_gzip
  read_json
}
```

If neither the URL nor the HTTP headers provide enough information, you can
explicitly specify the decompression and parsing steps using a pipeline
argument.
