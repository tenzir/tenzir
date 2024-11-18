# save_amqp

Saves a byte stream via AMQP messages.

```tql
save_amqp [url:str, channel=int, exchange=str, routing_key=str,
          options=record, mandatory=bool, immediate=bool]
```

## Description

The `save_amqp` operator is an [AMQP](https://www.amqp.org/) 0-9-1 client that
enables interacting with an AMQP server, as a *producer*.

The diagram below shows the key abstractions and how they relate to a pipeline:

![AMQP](amqp.excalidraw.svg)

The implementation of this connector relies on the [RabbitMQ C client
library](https://github.com/alanxz/rabbitmq-c).

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

For the loader, the name of the routing key to bind a queue to an exchange. For the saver, the routing key to publish messages with.

Defaults to the empty string.

### `options = record (optional)`

A comma-separated list of key-value configuration options for RabbitMQ, e.g.,
`{ max_channels: 42, frame_size: 1024, sasl_method: "external" }`. The example
`amqp.yaml` file below shows the available options:

import CodeBlock from '@theme/CodeBlock';
import Configuration from '!!raw-loader!@site/../plugins/amqp/amqp.yaml.example';

<CodeBlock language="yaml">{Configuration}</CodeBlock>

We recommend factoring the environment-specific options into the configuration
file so that they are not cluttering the pipeline definition.

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

Send the list of plugins as [JSON](write_json.md):

```tql
plugins
write_json
save_amqp
```
