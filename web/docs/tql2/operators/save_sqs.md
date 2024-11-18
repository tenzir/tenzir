# save_sqs

Saves bytes to [Amazon SQS][sqs] queues.

[sqs]: https://docs.aws.amazon.com/sqs/

```tql
save_sqs queue:str, [poll_time=duration]
```

## Description

[Amazon Simple Queue Service (Amazon SQS)][sqs] is a fully managed message
queuing service to decouple and scale microservices, distributed systems, and
serverless applications. The `save_sqs` operator writes bytes as messages into an SQS queue.

The `save_sqs` operator uses long polling, which helps reduce your cost of using SQS
by reducing the number of empty responses when there are no messages available
to return in reply to a message request. Use the `poll_time` option to adjust
the timeout.

### `queue: str`

The name of the queue to use.

### `poll_time = duration (optional)`

The long polling timeout per request.

The value must be between 1 and 20 seconds.

Defaults to `3s`.

## Examples

Write JSON messages from a source feed to the SQS queue `tenzir`:

```tql
subscribe "to-sqs"
write_json
save_sqs "sqs://tenzir"
```
