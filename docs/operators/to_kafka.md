---
title: to_kafka
category: Outputs/Events
example: 'to_kafka "topic", message=this.print_json()'
---

Sends messages to an Apache Kafka topic.

```tql
to_kafka topic:string, [message=blob|string, key=string, timestamp=time,
         options=record, aws_iam=record]
```

## Description

The `to_kafka` operator sends one message per event to a Kafka topic.

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

### `topic: string`

The Kafka topic to send messages to.

### `message = blob|string (optional)`

An expression that evaluates to the message content for each row.

Defaults to `this.print_json()` when not specified.

### `key = string (optional)`

Sets a fixed key for all messages.

### `timestamp = time (optional)`

Sets a fixed timestamp for all messages.

### `options = record (optional)`

A record of key-value configuration options for
[librdkafka][librdkafka], e.g., `{"acks": "all", "batch.size": 16384}`.

The `to_kafka` operator passes the key-value pairs directly to
[librdkafka][librdkafka]. Consult the list of available [configuration
options][librdkafka-options] to configure Kafka according to your needs.

We recommend factoring these options into the plugin-specific `kafka.yaml` so
that they are independent of the `to_kafka` arguments.

### `aws_iam = record (optional)`

If specified, enables using AWS IAM Authentication for MSK. The keys must be
non-empty when specified.

Available keys:

- `region`: Region of the MSK Clusters. Must be specified when using IAM.
- `assume_role`: Optional Role ARN to assume.
- `session_name`: Optional session name to use when assuming a role.
- `external_id`: Optional external id to use when assuming a role.

The operator will try to get credentials in the following order:

1. Checks your environment variables for AWS Credentials.
2. Checks your `$HOME/.aws/credentials` file for a profile and credentials
3. Contacts and logs in to a trusted identity provider. The login information to
   these providers can either be on the environment variables: `AWS_ROLE_ARN`,
   `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_SESSION_NAME` or on a profile in your
   `$HOME/.aws/credentials`.
4. Checks for an external method set as part of a profile on `$HOME/.aws/config`
   to generate or look up credentials that are not directly supported by AWS.
5. Contacts the ECS Task Role to request credentials if Environment variable
   `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` has been set.
6. Contacts the EC2 Instance Metadata service to request credentials if
   `AWS_EC2_METADATA_DISABLED` is NOT set to ON.

## Examples

### Send JSON-formatted events to topic `events` (using default)

```tql
subscribe "logs"
to_kafka "events"
```

### Send JSON-formatted events with explicit message

```tql
subscribe "logs"
to_kafka "events", message=this.print_json()
```

### Send specific field values with a timestamp

```tql
subscribe "logs"
to_kafka "alerts", message=alert_msg, timestamp=2024-01-01T00:00:00
```

### Send data with a fixed key for partitioning

```tql
metrics
to_kafka "metrics", message=this.print_json(), key="server-01"
```

## See Also

[`load_kafka`](/reference/operators/load_kafka),
[`save_kafka`](/reference/operators/save_kafka)
