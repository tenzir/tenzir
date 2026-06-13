---
title: Amazon Kinesis operators
type: feature
authors:
  - mavam
  - codex
prs:
  - 6188
created: 2026-05-15T19:17:21.013671Z
---

Tenzir can now read from and write to Amazon Kinesis data streams with the `from_amazon_kinesis` and `to_amazon_kinesis` operators:

```tql
from_amazon_kinesis "telemetry", start="trim_horizon"
this = string(message).parse_json()
```

```tql
read_ndjson
// ... transform events ...
to_amazon_kinesis "telemetry", partition_key=src_ip
```

The operators support existing AWS IAM configuration and endpoint overrides for local testing environments such as LocalStack.
