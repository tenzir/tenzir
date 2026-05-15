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
`from_cloudwatch` and `to_cloudwatch` operators. The source can subscribe to
live streams with `mode="live"`, page historical log groups with
`mode="filter"`, or read one stream with `mode="get"`.

```tql
from_cloudwatch "/aws/lambda/api", mode="filter", filter="ERROR"
```

The sink can send events with `PutLogEvents` or to the CloudWatch HTTP Log
Collector, including configurable batching, timestamp handling, parallel
requests, and AWS IAM authentication via `aws_iam`.

```tql
to_cloudwatch "/tenzir/events", "default", message=message, timestamp=ts
```
