---
title: Region derivation and endpoint logging for SQS
type: change
authors:
  - lava
prs:
  - 6168
created: 2026-05-13T09:08:28.12779Z
---

The `from_sqs` and `to_sqs` operators now derive the AWS region from a queue
URL when `aws_region` is not set, so passing a full URL such as
`https://sqs.us-west-2.amazonaws.com/123456789012/my-queue` works without
having to specify the region again:

```tql
from_sqs "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue"
```

Previously, this would fall back to the SDK default region and fail with a
SigV4 signature mismatch. Explicit `aws_region`, resolved IAM credentials, and
the SDK default still apply in that order when the URL has no region (for
example VPC endpoints, LocalStack, or an `AWS_ENDPOINT_URL` override).

SQS API errors and HTTP failures now also include the endpoint URL in their
log lines and diagnostic notes, which makes it easier to tell which queue
produced an error when multiple SQS pipelines run side by side.
