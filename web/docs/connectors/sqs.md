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

The `sqs` connector uses long polling, which helps reduce your cost of using SQS
by reducing the number of empty responses when there are no messages available
to return in reply to a message request. Use the `--poll-time` option to adjust
the timeout.

### `<queue>`

The name of the queue to use.

### `--poll-time <duration>`

The long polling timeout per request.

The `<duration>` value must be between 1 and 20 seconds.

Defaults to 10s.

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
