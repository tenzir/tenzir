---
title: from_kafka
category: Inputs/Events
example: 'from_kafka "logs"'
---

Receives events from an Apache Kafka topic.

```tql
from_kafka topic:string, [count=int, exit=bool, offset=int|string, options=record,
           aws_iam=record, commit_batch_size=int]
```

## Description

The `from_kafka` operator consumes messages from a Kafka topic and produces
events containing the message payload as a string field.

The implementation uses the official [librdkafka][librdkafka] from Confluent and
supports all [configuration options][librdkafka-options]. You can specify them
via `options` parameter as `{key: value, ...}`.

[librdkafka]: https://github.com/confluentinc/librdkafka
[librdkafka-options]: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

The operator injects the following default librdkafka configuration values in
case no configuration file is present, or when the configuration does not
include them:

- `bootstrap.servers`: `localhost`
- `client.id`: `tenzir`
- `group.id`: `tenzir`
- `enable.auto.commit`: `false` (This option cannot be changed)

Each consumed message is produced as an event with the following schema:

```tql
{
  message: string
}
```

### `topic: string`

The Kafka topic to consume from.

### `count = int (optional)`

Exit successfully after having consumed `count` messages.

### `exit = bool (optional)`

Exit successfully after having received the last message from all partitions.

Without this option, the operator waits for new messages after consuming the
last one.

### `offset = int|string (optional)`

The offset to start consuming from. Possible values are:

- `"beginning"`: first offset
- `"end"`: last offset
- `"stored"`: stored offset
- `<value>`: absolute offset
- `-<value>`: relative offset from end

The default is `"stored"`.

### `options = record (optional)`

A record of key-value configuration options for
[librdkafka][librdkafka], e.g., `{"auto.offset.reset" : "earliest",
"enable.partition.eof": true}`.

The `from_kafka` operator passes the key-value pairs directly to
[librdkafka][librdkafka]. Consult the list of available [configuration
options][librdkafka-options] to configure Kafka according to your needs.

We recommend factoring these options into the plugin-specific `kafka.yaml` so
that they are independent of the `from_kafka` arguments.

### `commit_batch_size = int (optional)`

The operator commits offsets after receiving `commit_batch_size` messages
to improve throughput. If you need to ensure exactly-once semantics for your
pipeline, set this option to `1` to commit every message individually.

Defaults to `1000`.

### `aws_iam = record (optional)`

If specified, enables using AWS IAM Authentication for MSK. The keys must be
non-empty when specified.

Available keys:

- `region`: Region of the MSK Clusters. Must be specified when using IAM.
- `assume_role`: Optional role ARN to assume.
- `session_name`: Optional session name to use when assuming a role.
- `external_id`: Optional external id to use when assuming a role.

The operator tries to get credentials in the following order:

1. Checks your environment variables for AWS Credentials.
2. Checks your `$HOME/.aws/credentials` file for a profile and credentials
3. Contacts and logs in to a trusted identity provider. The login information to
   these providers can either be on the environment variables: `AWS_ROLE_ARN`,
   `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_SESSION_NAME` or on a profile in your
   `$HOME/.aws/credentials`.
4. Checks for an external method set as part of a profile on `$HOME/.aws/config`
   to generate or look up credentials that isn't directly supported by AWS.
5. Contacts the ECS Task Role to request credentials if Environment variable
   `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` has been set.
6. Contacts the EC2 Instance Metadata service to request credentials if
   `AWS_EC2_METADATA_DISABLED` is NOT set to ON.

## Examples

### Consume JSON messages and parse them

```tql
from_kafka "logs"
message = message.parse_json()
```

### Consume 100 messages starting from the beginning

```tql
from_kafka "events", count=100, offset="beginning"
```

### Consume messages and exit when caught up

```tql
from_kafka "alerts", exit=true
```

### Consume from MSK using AWS IAM authentication

```tql
from_kafka "security-logs",
  options={"bootstrap.servers": "my-cluster.kafka.us-east-1.amazonaws.com:9098"},
  aws_iam={region: "us-east-1"}
```

## See Also

[`to_kafka`](/reference/operators/to_kafka)
