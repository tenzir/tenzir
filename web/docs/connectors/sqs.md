---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# sqs

Loads bytes from and saves bytes to [Amazon SQS][sqs] queues.

[sqs]: https://docs.aws.amazon.com/sqs/

## Synopsis

```
sqs [--poll-time <duration>] <queue>
```

## Description

[Amazon Simple Queue Service (Amazon SQS)][sqs] is a fully managed message
queuing service to decouple and scale microservices, distributed systems, and
serverless applications. The `sqs` loader reads bytes from messages of an
SQS queue. The `sqs` saver writes bytes as messages into an SQS queue.

The `sqs` connector uses short polling by default, querying only a subset of the
servers—based on a weighted random distribution—to determine whether any
messages are available for inclusion in the response. Add the `--poll-time`
option to activate long polling, which helps reduce your cost of using SQS by
reducing the number of empty responses when there are no messages available to
return in reply to a message request.

### `<queue>`

The name of the queue to use.

### `--poll-time <duration>`

Activates long polling. In combination with `--create`, the newly created queue
will have long polling enabled. When the queue exists already, the connector
will activate long polling for the queue by setting an attribute.

The `<duration>` value must be between 1 and 20 seconds.

By default, the connector uses short polling.

## Examples

Read JSON messages from the SQS queue `tenzir`:

```
from sqs://tenzir
```

Read JSON messages with a 20-second long poll timeout:

```
from sqs://tenzir --poll-time 20s
```

Write the Tenzir version 10 times, [enumerated](../operators/enumerate.md), to
queue `tenzir`:

```
version
| repeat 10
| enumerate
| to sqs://tenzir
```
