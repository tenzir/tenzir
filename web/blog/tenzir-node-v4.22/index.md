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

The new version of our query language - TQL2 - was first made
[public back in August of this year](tenzir-platform-is-now-generally-available#tql2).
You may also have noticed that the Tenzir App has had a toggle to enable TQL2 for
some time. Since then we have made great progress on improving its feature suite.

However, for most of that time there was no good way to learn about the new
language, as it lacked a crucial part: Documentation.

This changes today, as we are releasing the initial version of our
[TQL2 documentation](../overview).

As an example here is the
[page listing all operators currently available in TQL2](../tql2/operators).

Going forward, we are going to flesh out the documentation and fill in the
missing parts.

<!-- TODO: Do we encourage usage of TQL2 now? -->
<!-- TODO: Do we say that some new features may not be made for TQL1 any longer? -->

## Google Cloud Pub/Sub Integration

The new [`google-cloud-pubsub` connector](../next/connectors/google_cloud_pubsub) allows you to subscribe to
Google Cloud Pub/Sub subscriptions and publish to topics.

:::note Authenticate with the gcloud CLI
The connector tries to retrieve the appropriate credentials using
[Google's Application Default Credentials](https://google.aip.dev/auth/4110).
:::

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

The connector is also available in TQL2 `load_google_cloud_pubsub` and
`save_google_cloud_pubsub` respectively:

<!-- TODO: Write and link docs for these operators -->

```tql title="Using Tenzir to filter and translate events"
//tql2
load_google_cloud_pubsub "amazing-project-123456" "my-subscription"
read_syslog
content = content.parse_grok("{%WORD:type} %{IP:source}")
where content.type == "alert"
write_json ndjson=true
save_google_cloud_pubsub "amazing-project-123456" "alerts-topic"
```

## Other Changes

This release additionally includes numerous small bug fixes and under-the-hood
improvements. For a detailed list of changes, be sure to check out the
[changelog][changelog].

## Join Us for Office Hours

Every second Tuesday at 5 PM CET, we hold our office hours on our
[Discord server][discord]. Whether you have ideas for new packages or want to
discuss upcoming features—join us for a chat!

[discord]: /discord
[changelog]: /changelog#v4220
