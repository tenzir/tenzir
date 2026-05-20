---
title: CloudWatch Logs operators
type: feature
authors:
  - mavam
  - codex
prs:
  - 6180
created: 2026-05-14T21:05:10.807671Z
---

Tenzir now supports reading from and writing to CloudWatch Logs with the new
`from_amazon_cloudwatch` and `to_amazon_cloudwatch` operators. The source can
subscribe to live streams with `mode="live"`, search historical log groups with
`mode="search"`, or replay one stream with `mode="replay"`.

```tql
from_amazon_cloudwatch "/aws/lambda/api", mode="search", filter="ERROR"
```

The default sink can send events with `PutLogEvents`, including configurable
batching, timestamp handling, parallel requests, and AWS IAM authentication via
`aws_iam`. The sink can also write to the CloudWatch HTTP ingestion endpoints by
setting `method` to `json`, `ndjson`, or `hlc`, with either SigV4 or bearer-token
authentication.

```tql
to_amazon_cloudwatch "/tenzir/events",
  stream="default",
  payload=message,
  timestamp=ts
```
