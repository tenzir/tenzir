---
title: save_amqp
---

Saves a byte stream via AMQP messages.

```tql
save_amqp [url:str, channel=int, exchange=str, routing_key=str,
           options=record, mandatory=bool, immediate=bool]
```

## Description

The `save_amqp` operator is an [AMQP](https://www.amqp.org/) 0-9-1 client to
send messages to an exchange.

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

The routing key to publish messages with.

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

### `mandatory = bool (optional)`

This flag tells the server how to react if the message cannot be routed to a
queue. If `true`, the server will return an unroutable message with a Return
method. Otherwise the server silently drops the message.

Defaults to `false`.

### `immediate = bool (optional)`

This flag tells the server how to react if the message cannot be routed to a
queue consumer immediately. If `true`, the server will return an undeliverable
message with a Return method. If `false`, the server will queue the message, but
with no guarantee that it will ever be consumed.

Defaults to `false`.

## Examples

### Send the list of plugins as [JSON](/reference/operators/write_json)

```tql
plugins
write_json
save_amqp
```

## See Also

[`load_amqp`](/reference/operators/load_amqp)
