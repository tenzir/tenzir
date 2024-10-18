---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# google-cloud-pubsub

Loads bytes from a Google Cloud Subscription. Publishes bytes to a Google Cloud
Topic.

## Synopsis

Loader:

```
google-cloud-pubsub <project-id> <subscription-id> [--timeout=<duration>]
```

Saver:

```
google-cloud-pubsub <project-id> <topic-id>
```

## Description

The `google-cloud-pubsub` loader subscribes to a Google Cloud PubSub subscription,
the `google-cloud-pubsub` saver publishes to a Google Cloud PubSub topic.

The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).

### `<project-id>` (Loader, Saver)

The project to connect to. Be aware that this is the ID, and not the display name.

### `<subscription-id>` (Loader)

The subscription-id to subscribe to.

### `--timeout=<duration>` (Loader)

How long to wait for messages before ending the connection. A duration of zero means the subscription will run forever.

The default value is `0s`.

### `<topic-id>` (Saver)

The topic to publish to.

## Examples

Subscribe to `my-subscription` in the project `amazing-project-123456` and parse the messages as JSON:

```
load google-cloud-pubsub "amazing-project-123456" "my-subscription"
| parse json
```

Publish `suricata.alert` events as JSON the `alerts-topic` in the

```
export
| where #schema = 'suricata.alert'
| write json
| save google-cloud-pubsub "amazing-project-123456" "alerts-topic"
```
