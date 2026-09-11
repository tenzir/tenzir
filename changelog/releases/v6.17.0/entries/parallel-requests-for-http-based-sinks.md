---
title: Parallel requests for HTTP-based sinks
type: feature
authors:
  - aljazerzen
created: 2026-09-09T13:16:45.241765Z
---

The `to_splunk`, `to_opensearch`, `to_elasticsearch`, and `to_azure_log_analytics` operators now send several requests at the same time instead of waiting for each response before sending the next batch. The new `parallel` option bounds how many requests one operator instance keeps in flight:

```tql
to_splunk "https://splunk.example.org:8088", hec_token=secret("hec"), parallel=8
```

It defaults to `8`. Set `parallel=1` to restore the previous behavior of sending one request at a time. The `to_google_secops` operator, which already had a `parallel` option, now uses the same default of `8` instead of `50`.

These sinks are also parallelizable now, so pipelines that run with parallelism enabled replicate them across instances. Every instance buffers, authenticates, and sends on its own, which makes the pipeline-wide number of in-flight requests `parallel` times the degree of parallelism.

Both mechanisms give up the guarantee that the destination receives requests in the order the sink produced them.
