---
title: AWS IAM authentication for load_sqs, save_sqs, from_s3, to_s3, from_kafka,
  and to_kafka
type: feature
authors:
  - tobim
  - claude
pr: 5675
created: 2026-01-21T21:34:38.212368Z
---

The `load_sqs`, `save_sqs`, `from_s3`, `to_s3`, `from_kafka`, and `to_kafka` operators now support AWS IAM authentication through a new `aws_iam` option. You can configure explicit credentials, assume IAM roles, use AWS CLI profiles, or rely on the default credential chain.

The `aws_iam` option accepts these fields:

- `region`: AWS region for API requests (optional for SQS and S3, required for Kafka MSK)
- `profile`: AWS CLI profile name for credential resolution
- `access_key_id`: AWS access key ID
- `secret_access_key`: AWS secret access key
- `session_token`: AWS session token for temporary credentials
- `assume_role`: IAM role ARN to assume
- `session_name`: Session name for role assumption
- `external_id`: External ID for role assumption

You can also combine explicit credentials with role assumption. This uses the provided credentials to call STS AssumeRole and obtain temporary credentials for the assumed role:

```tql
load_sqs "my-queue", aws_iam={
  access_key_id: "AKIAIOSFODNN7EXAMPLE",
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  assume_role: "arn:aws:iam::123456789012:role/my-role"
}
```

For example, to load from SQS using explicit credentials:

```tql
load_sqs "my-queue", aws_iam={
  region: "us-east-1",
  access_key_id: "AKIAIOSFODNN7EXAMPLE",
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
}
```

To use an AWS CLI profile:

```tql
load_sqs "my-queue", aws_iam={
  profile: "production"
}
```

To assume an IAM role:

```tql
from_s3 "s3://bucket/path", aws_iam={
  assume_role: "arn:aws:iam::123456789012:role/my-role",
  session_name: "tenzir-session",
  external_id: "unique-id"
}
```

When no explicit credentials or profile are configured, operators use the AWS SDK's default credential provider chain, which checks environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), AWS configuration files (`~/.aws/credentials`), EC2/ECS instance metadata, and other standard sources. This applies both when `aws_iam` is omitted entirely and when `aws_iam` is specified without `access_key_id`, `secret_access_key`, or `profile`.
