# Notification stream of S3 events

This modules forwards EventBridge notifications for new objects in a source
`bucket_name` to a target event bus `target_bus_arn`.

We prefer using EventBridge rather than the legacy lambda notification system
because its pub/sub architecture allows us to subscribe to the notification feed
without disturbing existing notification configurations on the source bucket.

Note that we are not enabling EventBridge on the source bucket automatically
inside this module. Because of the structure of the [S3 notification
API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html),
the terraform `aws_s3_bucket_notification` resource replaces all existing
notification settings on the target bucket. We want to avoid that as the
asumption of this module is that the source bucket is not owned. Hence, for this
module to work, EventBridge must be enabled on the source bucket. This can be
achieved
[manually](https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-event-notifications-eventbridge.html)
or using Terragrunt hooks.
