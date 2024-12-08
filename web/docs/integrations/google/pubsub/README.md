# Pub/Sub

[Google Cloud Pub/Sub](https://cloud.google.com/pubsub) ingest events for
streaming into BigQuery, data lakes, or operational databases. Tenzir can act as
a publisher that sends messages to a topic, and as a subscriber that receives
messages from a subscription.

![Google Cloud Pub/Sub](google-cloud-pubsub.svg)

The operators
[`save_google_cloud_pubsub`](../../../tql2/operators/save_google_cloud_pubsub.md) and
and
[`load_google_cloud_pubsub`](../../../tql2/operators/load_google_cloud_pubsub.md)
implement subscriber and publisher, respectively. A message does not have any
structure and is just a block of bytes.

## Examples

### Publish a message to a topic

```tql
from {foo: 42}
write_json
save_google_cloud_pubsub project_id="amazing-project-123456", topic_id="alerts-topic"
```

### Receive messages from a subscription

```tql
load_google_cloud_pubsub project_id="amazing-project-123456", subscription_id="my-subscription"
read_json
```
