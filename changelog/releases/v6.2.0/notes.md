This release adds Amazon Kinesis operators, regex Kafka topic selection, Google SecOps Chronicle import support, and inline drop_null_fields cleanup for cloud and streaming pipelines. It also improves HTTP server bind failures, package field defaults, and timeout flushing for OpenSearch and CloudWatch sinks.

## 🚀 Features

### `drop_null_fields` function

The new `drop_null_fields` function strips fields whose value is `null` from a record, mirroring the existing `drop_null_fields` operator. This lets you clean optional fields inline in expressions, for example `from_http "...", body=drop_null_fields({license: $license, version: 1, mapping: $mapping})`.

*By @zedoraps and @claude in #6261.*

### Amazon Kinesis operators

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

*By @mavam and @codex in #6188.*

### Regular expression topic selection for from_kafka

The `from_kafka` operator can now consume from topics selected by a regular expression when the topic argument starts with `^`:

```tql
from_kafka "^tenant-.*\\.alerts$", offset="beginning"
```

This lets one pipeline consume matching Kafka topics without listing each topic separately.

As part of this change, `from_kafka` now consumes all topics—exact and regex—through Kafka's consumer group subscription. This has two visible effects. First, pipelines that share a `group.id` (default: `tenzir`) now split the partitions of a topic among themselves instead of each receiving every message; give each pipeline its own `group.id` to keep full copies. Second, partition assignments now follow group rebalances, so they may shift when pipelines with the same `group.id` start or stop. The `exit` option is not available for regex topics, because a regex subscription has no bounded set of partitions to reach the end of.

*By @mavam and @codex in #6262.*

### Selector defaults for UDO field parameters

Field-typed parameters of user-defined operators now accept selectors as defaults in the TQL frontmatter, such as `this`, `this.name`, or `foo.bar`. Previously, the only allowed default for a `field` parameter was `null`.

For example, this operator wraps a field into a record and operates on the entire event when no argument is given:

```tql
---
args:
  named:
    - name: field
      type: field
      default: this
---
this = {wrapped: $field}
```

Calling the operator without arguments wraps the whole event, while passing `field=name` wraps just that field. This makes it easy to write mapping operators that work on the full event by default but can be scoped to a subrecord on demand.

*By @mavam and @claude in #6352.*

## 🔧 Changes

### Google SecOps Chronicle import API

The `to_google_secops` operator now uses the Chronicle `logs.import`, `events.import`, and `entities.import` APIs instead of the legacy unstructured ingestion API.

The operator now targets a SecOps instance with `project`, `region`, and `instance`, authenticates with Google Cloud OAuth2 credentials, and supports raw log ingestion with `mode="raw_log"`, UDM event ingestion with `mode="udm_event"`, and entity ingestion with `mode="udm_entity"`.

*By @raxyte and @codex in #6216.*

## 🐞 Bug fixes

### HTTP server operators no longer crash the node on bind failure

The `accept_http`, `accept_opensearch`, and `serve_http` operators no longer abort the node when they fail to bind their endpoint. Starting a second pipeline on a port that is already in use—or restarting one before the previous instance has released its socket—previously crashed the entire node process. Now the operator emits a regular diagnostic:

```text
error: failed to start HTTP server: failed to bind to async server socket:
0.0.0.0:8774: Address already in use
```

The pipeline that hit the conflict exits with an error while every other pipeline running on the node keeps going.

*By @Zedoraps in #6267.*

### Nested assignment through package UDO field parameters

Package UDO field parameters now support assignment through field accesses. This lets package operators write to nested fields of a field argument using dot syntax, string-literal bracket syntax, or dynamic index expressions.

For example, this package UDO body is now valid:

```tql
$field.subfield = $value
$field["quoted-field"] = $value
$field[$key] = $value
```

This is useful for mapping operators that accept an `event=` target and need to keep temporary state under that target instead of creating top-level scratch fields.

*By @mavam, @jachris, and @codex in #6264.*

### Parallel signed AWS HTTP requests

AWS-backed operators that issue parallel signed HTTP requests now send concurrent requests over separate pooled connections.

Previously, HTTP/1.1 request pipelining could serialize parallel requests behind a single connection, so `parallel` settings for sinks such as `to_amazon_cloudwatch` did not provide the intended concurrency.

*By @mavam and @codex in #6188.*

### Timeout flushing for OpenSearch and CloudWatch sinks

The `to_opensearch` and `to_amazon_cloudwatch` sinks now reliably flush partial batches after their configured timeout while the pipeline continues to run.

Previously, concurrent wakeups could race with batch deadline updates, which could cause timeout-based flushes or CloudWatch send-completion handling to be missed.

*By @mavam and @codex in #6188.*
