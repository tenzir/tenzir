---
title: load_kafka
category: Inputs/Bytes
example: 'load_kafka topic="example"'
---

Loads a byte stream from a Apache Kafka topic.

```tql
load_kafka topic:string, [count=int, exit=bool, offset=int|string,
           options=record, aws_iam=record]
```

## Description

The `load_kafka` operator reads bytes from a Kafka topic.

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

### `topic: string`

The Kafka topic to use.

### `count = int (optional)`

Exit successfully after having consumed `count` messages.

### `exit = bool (optional)`

Exit successfully after having received the last message.

Without this option, the operator waits for new messages after having consumed the
last one.

### `offset = int|string (optional)`

The offset to start consuming from. Possible values are:

- `"beginning"`: first offset
- `"end"`: last offset
- `"stored"`: stored offset
- `<value>`: absolute offset
- `-<value>`: relative offset from end

<!--
- `s@<value>`: timestamp in ms to start at
- `e@<value>`: timestamp in ms to stop at (not included)
-->

### `options = record (optional)`

A record of key-value configuration options for
[librdkafka][librdkafka], e.g., `{"auto.offset.reset" : "earliest",
"enable.partition.eof": true}`.

The `load_kafka` operator passes the key-value pairs directly to
[librdkafka][librdkafka]. Consult the list of available [configuration
options][librdkafka-options] to configure Kafka according to your needs.

We recommand factoring these options into the plugin-specific `kafka.yaml` so
that they are indpendent of the `load_kafka` arguments.

### `aws_iam = record (optional)`

If specified, enables using AWS IAM Authentication for MSK. The keys must be
non-empty when specified.

Available keys:
- `region`: Region of the MSK Clusters. Must be specified when using IAM.
- `assume_role`: Optional role ARN to assume.
- `session_name`: Optional session name to use when assume a role.
- `external_id`: Optional external id to use when assuming a role.

The operator will try to get credentials in the following order:
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

### Read 100 JSON messages from the topic `tenzir`

```tql
load_kafka "tenzir", count=100
read_json
```

### Read Zeek Streaming JSON logs starting at the beginning

```tql
load_kafka "zeek", offset="beginning"
read_zeek_json
```

## See Also

[`save_kafka`](/reference/operators/save_kafka)
