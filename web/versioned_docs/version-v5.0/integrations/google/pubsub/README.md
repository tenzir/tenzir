# Pub/Sub

[Google Cloud Pub/Sub](https://cloud.google.com/pubsub) ingest events for
streaming into BigQuery, data lakes, or operational databases. Tenzir can act as
a publisher that sends messages to a topic, and as a subscriber that receives
messages from a subscription.

![Google Cloud Pub/Sub](google-cloud-pubsub.svg)

:::tip URL Support
The URL scheme `gcps://` dispatches to
[`load_google_cloud_pubsub`](../../../tql2/operators/load_google_cloud_pubsub.md)
and
[`save_google_cloud_pubsub`](../../../tql2/operators/save_google_cloud_pubsub.md)
for seamless URL-style use via [`from`](../../../tql2/operators/from.md) and
[`to`](../../../tql2/operators/to.md).
:::

## Examples

### Publish a message to a topic

```tql
from {foo: 42}
to "gcps://my-project/my-topic" {
  write_json
}
```

### Receive messages from a subscription

```tql
from "gcps://my-project/my-topic" {
  read_json
}
```
