---
title: "Tenzir Node v4.27: Amazon MSK IAM Integration"
slug: tenzir-node-v4.27
authors: [lava, raxyte]
date: 2025-01-30
tags: [release, node]
comments: true
---

Tenzir Node v4.27 enhances the charting capabilities and integrates with IAM for
authenticating to Amazon MSK.

![Tenzir Node v4.27](tenzir-node-v4.27.excalidraw.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.27.0

<!-- truncate -->

:::warning TQL1 Deprecation
TQL1 pipelines are deprecated starting from this release and the node will warn
on every execution of such pipelines. TQL2 is now in a much more mature state
and is the recommended way forward. [Read
more.](https://docs.tenzir.com/tql2-migration)
:::

## AWS IAM Authentication for MSK

[Amazon Managed Streaming for Apache Kafka (Amazon
MSK)](https://aws.amazon.com/msk/) is a streaming data service that manages
Apache Kafka infrastructure and operations, making it easier for developers and
DevOps managers to run Apache Kafka applications and Apache Kafka Connect
connectors on AWS without becoming experts in operating Apache Kafka.

Serverless MSK instances currently only support IAM Authentication, which means
you could not communicate with them using Tenzir. This unfortunate situation has
now changed!

With this release, the `load_kafka` and `save_kafka` operators can now
authenticate with MSK using AWS IAM by simply specifying the `aws_iam`
option with a record of configuration values such as:

```tql
load_kafka "kafkaesque-data", aws_iam={region: "eu-west-1"}
```

The above pipeline will try to fetch credentials from [various different
locations](/next/tql2/operators/load_kafka#aws_iam--record-optional) including
Instance Metadata Services. This means you can attach a role with the necessary
permissions directly to an EC2 instance and Tenzir will automatically pick it up.

### Assuming roles

Roles can also be assumed by giving the `assume_role` parameter to the `aws_iam` option.

```tql
save_kafka "topic", aws_iam={region: "eu-west-1", assume_role: "arn:aws:iam::1234567890:role/my-msk-role"}
```

The above pipeline attempts to fetch temporary credentials from Amazon STS for
the given ARN.

### Example

#### Collecting High Severity OCSF events from MSK

```tql
let $endpoints = ["indexer-1-url", "indexer-2-url"]

load_kafka "ocsf-events", aws_iam={region: "us-east-2", assume_role: "arn"}
read_json
where severity_id >= 4 // High and above
load_balance $endpoints {
    to_splunk $endpoints, hec_token=secret("SPLUNK_TOKEN")
}
```

The above pipeline reads OCSF events from MSK, assuming the role referenced by
the provided ARN. The incoming data is then filtered for severity and sent to
Splunk clusters in a load balanced fashion.

## Charts, Retention and TLS

This release also includes a number of other notable features for the Tenzir Node.

### Charts

This release brings over the family of familiar charting operators from TQL1
with some new delightful features. The new operators allow you to group by
different fields or choose a resolution for a time-series-like data and more!

We explore charting in more detail in our upcoming Tenzir Platform v1.8
release blog post, so stay tuned.

### Retention

Two new settings `tenzir.retention.metrics` and `tenzir.retention.diagnostics` that
control the retention time of metrics and diagnostics.

These settings are duration-valued and the Tenzir Node will automatically delete
the internally stored metrics and diagnostics events after the configured amount
of time has passed.

### TLS

We've added new options for establishing the connection to the Tenzir Platform
that make it easier to use the Tenzir Node in self-hosted environments with
private certificate authorities.

The `tenzir.platform.cacert` option points to a file containing one or more
CA certificates that are used for validating the certificate presented by
the platform.

The `tenzir.platform.skip-peer-verification` option can be enabled in order to
connect to a Tenzir Platform instance that is using self-signed certificates.

```sh
TENZIR_PLATFORM__CACERT=/path/to/certificates.crt
TENZIR_PLATFORM__SKIP_PEER_VERIFICATION=true/false
```

Note that these settings only apply to the connection made from
the Tenzir Node to the Tenzir Platform on startup, and not to
any outgoing HTTP connections made by individual pipelines.

## Let's Connect!

We’re excited to engage with our community!
Join us every second Tuesday at 5 PM CET for office hours on [Discord][discord].
Share your ideas, preview upcoming features, or chat with fellow Tenzir users
and our team. Bring your questions, use cases, or just stop by to say hello!

[discord]: /discord
[changelog]: /changelog#v4270
