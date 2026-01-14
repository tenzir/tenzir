---
title: "Backpressure and connection limits for HTTP server"
type: feature
author: raxyte
created: 2025-12-12T09:10:39Z
pr: 5601
---

The `from_http` operator in server mode now implements backpressure, waiting for
each request to be processed before accepting new data. This prevents memory
pressure during traffic spikes from webhook integrations or log receivers.

A new `max_connections` parameter limits simultaneous connections:

```tql
from_http "0.0.0.0:8080", server=true, max_connections=50
```

The default is 10 connections. Additional connections are rejected until a slot
frees up, keeping your pipelines stable under heavy load.
