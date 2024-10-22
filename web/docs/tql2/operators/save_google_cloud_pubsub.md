# save_google_cloud_pubsub

Publishes to a Google Cloud Pub/Sub topic.

```tql
save_google_cloud_pubsub project_id:str, topic_id:str
```

:::note Authentication
The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).
:::

## Description

The `google_cloud_pubsub` saver publishes bytes to a Google Cloud Pub/Sub topic.

### `project_id: str`

The project to connect to. Note that this is the project_id, not the display name.

### `topic_id: str`

The topic to publish to.

## Examples

Publish `suricata.alert` events as JSON to `alerts-topic`.

```tql
export
where @name = "suricata.alert"
write_json
save_google_cloud_pubsub "amazing-project-123456", "alerts-topic"
```
