---
title: Tenzir Node v4.22
authors: [jachris, raxyte]
date: 2024-10-18
tags: [release, node]
comments: true
---

[Tenzir Node v4.22][github-release] comes with documentation for the new version
of the Tenzir Query Language, connectors for Google Cloud Pub/Sub and various
bug fixes.

![Tenzir Node v4.22](tenzir-node-v4.22.excalidraw.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.22.0

<!-- truncate -->

## TQL2 Documentation

We are thrilled to share that the new version of our query language, TQL2,
has taken exciting strides forward! If you’ve been using the Tenzir Platform,
you might have already noticed the toggle to enable TQL2,
allowing you to explore its potential.

Today marks a significant milestone as we release the initial version of our
[TQL2 documentation](../next/tql2/operators). You can now dive deep into
learning TQL2 and unlock its full capabilities!

We’re committed to continuously enhancing the documentation, ensuring you have
all the resources you need to make the most of TQL2. Stay tuned for more
updates!

## Google Cloud Pub/Sub Integration

Introducing our latest connector: Google Cloud Pub/Sub! The new
[`google-cloud-pubsub` connector](../next/connectors/google_cloud_pubsub) allows
users to seamlessly subscribe and publish to Google Cloud Pub/Sub.

```text{0} title="Subscribe to 'my-subscription'"
load google-cloud-pubsub "amazing-project-123456" "my-subscription"
| parse syslog
...
```

```text{0} title="Publish events to 'alerts-topic'"
...
| write json --ndjson
| save google-cloud-pubsub "amazing-project-123456" "alerts-topic"
```

The connector is also available in TQL2 as
[`load_google_cloud_pubsub`](../next/tql2/operators/load_google_cloud_pubsub) and
[`save_google_cloud_pubsub`](../next/tql2/operators/save_google_cloud_pubsub):

```tql title="Using Tenzir to filter and convert events"
load_google_cloud_pubsub "amazing-project-123456", "my-subscription"
read_syslog
content = content.parse_grok("{%WORD:type} %{IP:source}")
where content.type == "alert"
write_json ndjson=true
save_google_cloud_pubsub "amazing-project-123456", "alerts-topic"
```

## Other Changes

This release additionally includes numerous small bug fixes and under-the-hood
improvements. For a detailed list of changes, be sure to check out the
[changelog][changelog].

## Join Us for Office Hours

Every second Tuesday at 5 PM CET, we hold our office hours on our [Discord
server][discord]. Whether you have ideas for new packages or want to discuss
upcoming features—join us for a chat!

[discord]: /discord
[changelog]: /changelog#v4220
