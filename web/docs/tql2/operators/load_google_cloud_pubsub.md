# load_google_could_pubsub

Subscribes to a Google Cloud Pub/Sub subscription and obtains bytes.

```tql
load_google_could_pubsub project_id:str, subscription_id:str [timeout:duration]
```

:::note Authentication
The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).
:::


## Description

The `google_cloud_pubsub` loader acquires raw bytes from a Google Cloud Pub/Sub subscription

### `project_id: str`

The project to connect to. Be aware that this is the ID, and not the display name.

### `subscription_id: str`

The subscription-id to subscribe to.

### `timeout = duration (optional)`

How long to wait for messages before ending the connection. A duration of zero means the operator will run forever.

The default value is `0s`.

## Examples

Subscribe to `my-subscription` in the project `amazing-project-123456` and parse the messages as JSON:

```tql
load_google_cloud_pubsub "amazing-project-123456" "my-subscription"
| parse_json
```
