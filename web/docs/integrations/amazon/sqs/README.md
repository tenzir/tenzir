# SQS

[Amazon Simple Queuing Service (SQS)](https://aws.amazon.com/sqs/) is a managed
message queue for microservices, distributed systems, and serverless
applications.

Tenzir can interact with SQS by sending messages to and reading messages from
SQS queues.

![SQS](sqs.svg)

When reading from SQS queues, you cannot specify individual messages. Instead,
you determine the maximum number of messages you wish to retrieve, up to a limit
of 10.

If the parameter `poll_interval` is non-zero, the pipeline automatically
performs [long
polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html).
This means that you may receive messages in bursts, according to your specified
poll interval.

Tenzir pipelines that read from an SQS queue automatically send a deletion
request after having received the messages.

:::tip URL Support
The URL scheme `sqs://` dispatches to
[`load_sqs`](../../../tql2/operators/load_sqs.md) and
[`save_sqs`](../../../tql2/operators/save_sqs.md) for seamless URL-style use via
[`from`](../../../tql2/operators/from.md) and
[`to`](../../../tql2/operators/to.md).
:::

## Configuration

Follow the [standard configuration instructions](..) to authenticate
with your AWS credentials.

## Examples

### Send a message to an SQS queue

```tql
from {foo: 42}
to "sqs://my-queue" {
  write_json
}
```

### Receive messages from an SQS queue

```tql
from "sqs://my-queue", poll_interval=5s {
  read_json
}
```

```tql
{foo: 42}
```
