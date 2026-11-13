---
title: Native CloudWatch Logs semantics for operators after from_amazon_cloudwatch
type: change
authors:
  - mavam
created: 2026-11-11T15:55:04.903156Z
---

`from_amazon_cloudwatch` now hands CloudWatch Logs the operators after it that its request can express: filter patterns, time windows, log streams, and limits. They leave the pipeline and run in CloudWatch Logs, with its semantics. For example, this pipeline:

```tql
from_amazon_cloudwatch "/aws/lambda/checkout", mode="search"
where "ERROR" in message
where not ("timeout" in message)
where timestamp >= 2026-01-01T12:00:00Z
where log_stream in ["2026/01/01/[$LATEST]abc", "2026/01/01/[$LATEST]def"]
head 100
```

sends this `FilterLogEvents` request:

```json
{
  "logGroupName": "/aws/lambda/checkout",
  "logStreamNames": ["2026/01/01/[$LATEST]abc", "2026/01/01/[$LATEST]def"],
  "startTime": 1767268800000,
  "filterPattern": "\"ERROR\" -\"timeout\"",
  "limit": 100
}
```

With `mode="live"`, `where "ERROR" in message or "FATAL" in message` becomes the Live Tail pattern `?"ERROR" ?"FATAL"`. A live read also warns when Live Tail samples the matching events. Use `pushdown=false` to keep every operator in the pipeline with TQL semantics.
