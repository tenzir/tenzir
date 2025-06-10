---
title: "Implement `from_opensearch`"
type: feature
authors: raxyte
pr: 5075
---

We added a `from_opensearch` operator that presents a OpenSearch-compatible REST
API to enable easy interop with tools that can send data to OpenSearch or
Elasticsearch, e.g. Filebeat, Winlogbeat etc.
