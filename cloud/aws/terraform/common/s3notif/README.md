# Notification stream of S3 events

This modules forwards EventBridge notifications for new objects in a source
`bucket_name` to a target event bus `target_bus_arn`.

We prefer using EventBridge rather than the legacy lambda notification system
because its pub/sub architecture allows us to subscribe to the notification feed
without disturbing existing notification configurations on the source bucket.

Note that for this module to work, EventBridge must be enabled on the source
bucket. This can be achieved manually or using Terragrunt hooks.
