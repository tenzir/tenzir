---
title: load_google_cloud_pubsub
category: Inputs/Bytes
example: 'load_google_cloud_pubsub project_id="my-project"'
---

Subscribes to a Google Cloud Pub/Sub subscription and obtains bytes.

```tql
load_google_cloud_pubsub project_id=string, subscription_id=string, [timeout=duration]
```

:::note[Authentication]
The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).
:::

## Description

:::caution[Deprecated]
`load_google_cloud_pubsub` is deprecated. Use
[`from_google_cloud_pubsub`](/reference/operators/from_google_cloud_pubsub),
which preserves event boundaries and supports attaching metadata.
:::

The operator acquires raw bytes from a Google Cloud Pub/Sub subscription.

### `project_id = string`

The project to connect to. Note that this is the project id, not the display name.

### `subscription_id = string`

The subscription to subscribe to.

### `timeout = duration (optional)`

How long to wait for messages before ending the connection. A duration of zero
means the operator will run forever.

The default value is `0s`.

## URI support & integration with `from`

The `load_google_cloud_pubsub` operator can also be used from the [`from`](/reference/operators/from)
operator. For this, the `gcps://` scheme can be used. The URI is then translated:

```tql
from "gcps://my_project/my_subscription"
```
```tql
load_google_cloud_pubsub project_id="my_project", subscription_id="my_subscription"
```

## Examples

### Read JSON messages from a subscription

Subscribe to `my-subscription` in the project `amazing-project-123456` and parse
the messages as JSON:

```tql
load_google_cloud_pubsub project_id="amazing-project-123456", subscription_id="my-subscription"
read_json
```

## See Also

[`from_google_cloud_pubsub`](/reference/operators/from_google_cloud_pubsub),
[`save_google_cloud_pubsub`](/reference/operators/save_google_cloud_pubsub)
