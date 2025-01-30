# MSK

[Amazon Managed Streaming for Apache Kafka (Amazon
MSK)](https://aws.amazon.com/msk/) is a streaming data service that manages
Apache Kafka infrastructure and operations, making it easier for developers and
DevOps managers to run Apache Kafka applications and Apache Kafka Connect
connectors on AWS without becoming experts in operating Apache Kafka.

## Sending and Receiving

Tenzir's Kafka connectors `load_kafka` and `save_kafka` can be used to send and
receive events from Amazon MSK Clusters.

## Authentication

Provisioned MSK Clusters support different authentication mechanisms such as
mTLS, SASL/SCRAM, IAM etc. However Serverless MSK instances currently only
support IAM Authentication.

The `load_kafka` and `save_kafka` operators can authenticate with MSK using AWS
IAM by simply specifying the `aws_iam` option with a record of configuration
values such as:

```tql
load_kafka "kafkaesque-data", aws_iam={region: "eu-west-1"}
```

The above pipeline will try to fetch credentials from [various different
locations](/next/tql2/operators/load_kafka#aws_iam--record-optional) including
the Instance Metadata Services. This means you can attach a role with the
necessary permissions directly to an EC2 instance and Tenzir will automatically
pick it up.

:::tip Other Authentication methods
Tenzir relies on [librdkafka](https://github.com/confluentinc/librdkafka) for
the Kafka client protocol and all other mechanisms supported by librdkafka can
be used by specifying respective configuration values, e.g.
[mTLS](https://github.com/confluentinc/librdkafka/wiki/Using-SSL-with-librdkafka).
:::

### Assuming roles

Roles can also be assumed by giving the `assume_role` parameter to the the `aws_iam` option.

```tql
save_kafka "topic", aws_iam={region: "eu-west-1", assume_role: "arn:aws:iam::1234567890:role/my-msk-role"}
```

The above pipeline attempts to fetch temporary credentials from Amazon STS for
the given ARN.

### Example

#### Collecting High Severity OCSF events from MSK

The following pipeline reads OCSF events from MSK, assuming the role referenced by
the provided ARN. The incoming data is then filtered for severity and sent to
Splunk clusters in a load balanced fashion.

```tql
let $endpoints = ["indexer-1-url", "indexer-2-url"]

load_kafka "ocsf-events", aws_iam={region: "us-east-2", assume_role: "arn"}
read_json
where severity_id >= 4 // High and above
load_balance $endpoints {
    to_splunk $endpoints, hec_token=secret("SPLUNK_TOKEN")
}
```
