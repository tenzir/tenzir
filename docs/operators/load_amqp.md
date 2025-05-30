---
title: load_amqp
---

Loads a byte stream via AMQP messages.

```tql
load_amqp [url:str, channel=int, exchange=str, routing_key=str, queue=str,
           options=record, passive=bool, durable=bool, exclusive=bool,
           no_auto_delete=bool, no_local=bool, ack=bool]
```

## Description

The `load_amqp` operator is an [AMQP](https://www.amqp.org/) 0-9-1 client to
receive messages from a queue.

### `url: str (optional)`

A URL that specifies the AMQP server. The URL must have the following format:

```
amqp://[USERNAME[:PASSWORD]@]HOSTNAME[:PORT]/[VHOST]
```

When the URL is present, it will overwrite the corresponding values of the
configuration options.

### `channel = int (optional)`

The channel number to use.

Defaults to `1`.

### `exchange = str (optional)`

The exchange to interact with.

Defaults to `"amq.direct"`.

### `routing_key = str (optional)`

The name of the routing key to bind a queue to an exchange.

Defaults to the empty string.

### `options = record (optional)`

An option record for RabbitMQ , e.g., `{max_channels: 42, frame_size: 1024,
sasl_method: "external"}`.

Available options are:

```yaml
hostname: 127.0.0.1
port: 5672
ssl: false
vhost: /
max_channels: 2047
frame_size: 131072
heartbeat: 0
sasl_method: plain
username: guest
password: guest
```

### `queue = str (optional)`

The name of the queue to declare and then bind to.

Defaults to the empty string, resulting in auto-generated queue names, such as
`"amq.gen-XNTLF0FwabIn9FFKKtQHzg"`.

### `passive = bool (optional)`

If `true`, the server will reply with OK if an exchange already exists with the
same name, and raise an error otherwise.

Defaults to `false`.

### `durable = bool (optional)`

If `true` when creating a new exchange, the exchange will be marked as durable.
Durable exchanges remain active when a server restarts. Non-durable exchanges
(transient exchanges) are purged if/when a server restarts.

Defaults to `false`.

### `exclusive = bool (optional)`

If `true`, marks the queue as exclusive. Exclusive queues may only be accessed by
the current connection, and are deleted when that connection closes. Passive
declaration of an exclusive queue by other connections are not allowed.

Defaults to `false`.

### `no_auto_delete = bool (optional)`

If `true`, the exchange will *not* be deleted when all queues have finished using
it.

Defaults to `false`.

### `no_local = bool (optional)`

If `true`, the server will not send messages to the connection that published them.

Defaults to `false`.

### `ack = bool (optional)`

If `true`, the server expects acknowledgements for messages. Otherwise, when a
message is delivered to the client the server assumes the delivery will succeed
and immediately dequeues it. This functionality may decrease performance, while
improving reliability. Without this flag, messages can get lost if a client
dies before they are delivered to the application.

Defaults to `false`.

## Examples

### Consume a message from a specified AMQP queue

```tql
load_amqp "amqp://admin:pass@0.0.0.1:5672/vhost", queue="foo"
read_json
```

## See Also

[`save_amqp`](/reference/operators/save_amqp)
