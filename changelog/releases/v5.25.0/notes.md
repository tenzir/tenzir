This release adds periodic emission to the summarize operator, enabling real-time streaming analytics with configurable intervals and accumulation modes. It also introduces AWS IAM authentication across SQS, S3, and Kafka operators, and fixes memory instability in from_http when used with slow downstream consumers.

## 🚀 Features

### AWS IAM authentication for load_sqs, save_sqs, from_s3, to_s3, from_kafka, and to_kafka

The `load_sqs`, `save_sqs`, `from_s3`, `to_s3`, `from_kafka`, and `to_kafka` operators now support AWS IAM authentication through a new `aws_iam` option. You can configure explicit credentials, assume IAM roles, use AWS CLI profiles, or rely on the default credential chain.

The `aws_iam` option accepts these fields:

- `profile`: AWS CLI profile name for credential resolution
- `access_key_id`: AWS access key ID
- `secret_access_key`: AWS secret access key
- `session_token`: AWS session token for temporary credentials
- `assume_role`: IAM role ARN to assume
- `session_name`: Session name for role assumption
- `external_id`: External ID for role assumption

Additionally, the SQS and Kafka operators accept a top-level `aws_region` option:

- For `load_sqs` and `save_sqs`: Configures the AWS SDK client region for queue URL resolution
- For `from_kafka` and `to_kafka`: Required for MSK authentication (used to construct the authentication endpoint URL)

You can also combine explicit credentials with role assumption. This uses the provided credentials to call STS AssumeRole and obtain temporary credentials for the assumed role:

```tql
load_sqs "my-queue", aws_iam={
  access_key_id: "AKIAIOSFODNN7EXAMPLE",
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  assume_role: "arn:aws:iam::123456789012:role/my-role"
}
```

For example, to load from SQS with a specific region:

```tql
load_sqs "my-queue", aws_region="us-east-1", aws_iam={
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

For Kafka MSK authentication, the `aws_region` option is required:

```tql
from_kafka "my-topic", aws_region="us-east-1", aws_iam={
  profile: "production"
}
```

When no explicit credentials or profile are configured, operators use the AWS SDK's default credential provider chain, which checks environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), AWS configuration files (`~/.aws/credentials`), EC2/ECS instance metadata, and other standard sources. This applies both when `aws_iam` is omitted entirely and when `aws_iam` is specified without `access_key_id`, `secret_access_key`, or `profile`.

*By @tobim and @claude in #5675.*

### Per-actor memory allocation tracking

We have added support for per-actor/per-thread allocation tracking. When enabled, these stats will track which actor (or thread) allocated how much memory. This gives much more detailed insights into where memory is allocated. By default these detailed statistics are not collected, as they introduce a cost to every allocation.

*By @IyeOnline in #5646.*

### Periodic emission for summarize operator

The `summarize` operator now supports periodic emission of aggregation results at fixed intervals, enabling real-time streaming analytics and monitoring use cases.

Use the `options` named argument with `frequency` to emit results every N seconds:

```tql
summarize count(this), src_ip, options={frequency: 5s}
```

This emits aggregation results every 5 seconds, showing the count per source IP for events received during each interval:

```tql
{src_ip: 192.168.1.1, count: 42}
{src_ip: 192.168.1.2, count: 17}
// ... 5 seconds later ...
{src_ip: 192.168.1.1, count: 38}
{src_ip: 192.168.1.3, count: 9}
```

The `mode` parameter controls how aggregations behave across emissions:

**Reset mode** (default) resets aggregations after each emission, providing per-interval metrics:

```tql
summarize sum(bytes), options={frequency: 10s}
// Shows bytes per 10-second window
```

**Cumulative mode** accumulates values across emissions, providing running totals:

```tql
summarize sum(bytes), options={frequency: 10s, mode: "cumulative"}
// Shows total bytes seen so far
```

**Update mode** emits only when values change from the previous emission, reducing output noise in monitoring scenarios:

```tql
summarize count(this), severity, options={frequency: 1s, mode: "update"}
// Emits only when the count for a severity level changes
```

The operator always emits final results when the input stream ends, ensuring no data is lost.

*By @tobim and @claude in #5605.*

### RFC 6587 octet-counting support for syslog parsing

The `parse_syslog` function now supports RFC 6587 octet-counted framing, where syslog messages are prefixed with their byte length (for example, `65 <syslog-message>`). This framing is commonly used in TCP-based syslog transport to handle message boundaries.

The new `octet_counting` parameter for `parse_syslog` offers three modes:

- **Not specified (default)**: Auto-detect. The parser strips a length prefix if present and valid, otherwise parses the input as-is. This prevents false positives where input coincidentally starts with digits and a space.
- **`octet_counting=true`**: Require a length prefix. Emits a warning and returns null if the input lacks a valid prefix.
- **`octet_counting=false`**: Never strip a length prefix. Parse the input as-is.

*By @mavam and @claude.*

## 🔧 Changes

### Cleanup of existing directory markers in from_s3 and from_abs

The `from_s3` and `from_azure_blob_storage` operators now also delete existing directory marker objects along the glob path when `remove=true`. Directory markers are zero-byte objects with keys ending in `/` that some cloud storage tools create. These artifacts can accumulate over time, increasing API costs and slowing down listing operations.

*By @jachris in #5670.*

### Preserve original field order in ocsf::derive

The `ocsf::derive` operator now preserves original field order instead of reordering alphabetically. Derived enum/sibling pairs are inserted at the position of the first field, ordered alphabetically within each pair (e.g., `activity_id` before `activity_name`). Non-OCSF fields remain at their original positions.

For example, given the input:

```tql
{foo: 1, class_uid: 1001}
```

The output is now:

```tql
{foo: 1, class_name: "...", class_uid: 1001}
```

Previously, the output was alphabetically sorted:

```tql
{class_name: "...", class_uid: 1001, foo: 1}
```

*By @mavam and @claude in #5673.*

## 🐞 Bug fixes

### Correct multi-partition commits in `from_kafka`

The `from_kafka` operator now commits offsets per partition and tracks partition EOFs based on the current assignment, preventing premature exits and cross-partition replays after restarts.

*By @raxyte and @codex in #5654.*

### No more directory markers for S3 and Azure

Deleting files from S3 or Azure Blob Storage via `from_s3` or `from_azure_blob_storage` with the `remove=true` option no longer creates empty directory marker objects in the parent directory when the last file of the directory is deleted.

*By @jachris in #5669.*

### Phantom pipeline entries with empty IDs

In rare cases, a phantom pipeline with an empty ID could appear in the pipeline list that couldn't be deleted through the API.

*By @jachris and @claude in #5680.*

### Stable memory usage for `from_http` server

The `from_http` server now has a stable memory usage when used with a slow downstream, especially in situations where the client timeouts and retries requests.

*By @raxyte in #5677.*
