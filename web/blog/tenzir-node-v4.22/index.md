---
title: Tenzir Node v4.22
authors: [jachris, raxyte]
date: 2024-10-17
tags: [release, node]
comments: true
---

The Documentation for the new version of our Tenzir Query Language is finally here!

<!-- ![Tenzir Node v4.22](tenzir-node-v4.21.excalidraw.svg) -->

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.22.0

<!-- truncate -->

## TQL2 Documentation


## Publish and Subscribe to Google Cloud Pub/Sub

The new `google-cloud-pubsub` connector allows you to subscribe to
Google Cloud Pub/Sub subscriptions and publish to topics.

:::note Authenticate with the gcloud CLI
The connector tries to retrieve the appropriate credentials using
[Google's Application Default Credentials](https://google.aip.dev/auth/4110).
:::

```text{0} title="Subscribe to 'my-subscription'"
load google-cloud-pubsub "amazing-project-123456" "my-subscription"
| parse json
```

```text{0} title="Export all 'suricata.alert' events to 'alerts-topic'"
export
| where #schema = "suricata.alert"
| write json
| save google-cloud-pubsub "amazing-project-123456" "alerts-topic"
```

The connector is also available in TQL2 `load_google_cloud_pubsub` and
`save_google_cloud_pubsub` respectively:

```tql title="Using Tenzir to filter and translate events"
//tql2
load_google_cloud_pubsub "amazing-project-123456" "syslog-subscription"
read_syslog
content = content.parse_grok("{%WORD:type} %{IP:source}")
where content.type == "alert"
write nd_json
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
[changelog]: /changelog#v4210
