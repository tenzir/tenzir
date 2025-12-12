---
title: from_google_cloud_pubsub
category: Inputs/Events
example: 'from_google_cloud_pubsub project_id="my-project", subscription_id="my-sub"'
---

Subscribes to a Google Cloud Pub/Sub subscription and yields events.

```tql
from_google_cloud_pubsub project_id=string, subscription_id=string, [metadata_field=field]
```

:::note[Authentication]
The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).
:::

## Description

The operator emits one event per Pub/Sub message with a single field `message`
containing the message payload as a string.

### `project_id = string`

The project to connect to. This must be the project ID, not the display name.

### `subscription_id = string`

The subscription to subscribe to.

### `metadata_field = field (optional)`

When set, delivery metadata is attached at the given field path. Each event then
also contains a record with this schema:

```tql
{
  message_id: string, // Pub/Sub message ID
  publish_time: time, // Message publish time
  attributes: { // Record containing all attributes of the message
    key: string ...
  }
}
```

## Examples

### Consume messages and parse JSON

```tql
from_google_cloud_pubsub project_id="amazing-project-123456", subscription_id="my-subscription"
this = message.parse_json()
```

### Collect metadata alongside the message

```tql
from_google_cloud_pubsub project_id="amazing-project-123456", subscription_id="my-subscription", metadata_field=metadata
```
```
{
  message: "some text",
  metadata: {
    message_id: "0",
    publish_time: 2025-12-12T17:05:07.324958101Z,
    attributes: {},
  }
}
```

## See Also

[`to_google_cloud_pubsub`](/reference/operators/to_google_cloud_pubsub)
