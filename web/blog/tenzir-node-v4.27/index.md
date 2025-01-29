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
permissions directly to an EC2 node and Tenzir will automatically pick it up.

### Assuming roles

Roles can also be assumed by giving the `assume_role` parameter to the the `aws_iam` option.

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

## TQL2

TODO: Maybe a general update on timeline for missing features?

## Charts, Retention and TLS

This release brings over the family of familiar charting operators from TQL1
with some new delightful features. The new operators allow you to group by
different fields or choose a resolution for a time-series-like data and more!

TODO: Mention the other changes

Stay tuned for our upcoming blog post for more details.

## Let's Connect!

We’re excited to engage with our community!
Join us every second Tuesday at 5 PM CET for office hours on [Discord][discord].
Share your ideas, preview upcoming features, or chat with fellow Tenzir users
and our team. Bring your questions, use cases, or just stop by to say hello!

[discord]: /discord
[changelog]: /changelog#v4270
