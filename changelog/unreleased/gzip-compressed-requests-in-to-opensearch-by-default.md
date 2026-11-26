---
title: Gzip-compressed requests in to_opensearch by default
type: change
authors:
  - jachris
created: 2026-11-25T18:22:38.59838Z
---

The `to_opensearch` and `to_elasticsearch` operators now compress request bodies with gzip by default, which reduces the bytes sent over the network. OpenSearch and Elasticsearch decompress these requests transparently.

To send uncompressed requests, for example to a proxy that does not support compressed request bodies, set `compress=false`:

```tql
to_opensearch "https://opensearch.example.com:9200",
  action="index",
  index="main",
  compress=false
```
