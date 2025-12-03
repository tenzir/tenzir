---
title: to_google_cloud_pubsub
category: Outputs/Events
example: 'to_google_cloud_pubsub project_id="my-project", topic_id="alerts", message=.text'
---

Publishes events to a Google Cloud Pub/Sub topic.

```tql
to_google_cloud_pubsub project_id=string, topic_id=string, message=string
```

:::note[Authentication]
The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).
:::

## Description

The operator publishes one Pub/Sub message per input event. The message content
comes from the `message` expression, which must evaluate to a `string`.

### `project_id = string`

The project to connect to. This must be the project ID, not the display name.

### `topic_id = string`

The Pub/Sub topic to publish to.

### `message = string`

A string to publish as the message.

## Examples

### Send alert text to a topic

Publish the `alert_text` field of every event to `alerts-topic`:

```tql
export
where @name == "suricata.alert"
to_google_cloud_pubsub project_id="amazing-project-123456", topic_id="alerts-topic", message=this.print_ndjson()
```

## See Also

[`from_google_cloud_pubsub`](/reference/operators/from_google_clould_pubsub),
