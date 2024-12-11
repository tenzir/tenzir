# load_amqp

Loads a byte stream via AMQP messages.

```tql
load_amqp [url:str, channel=int, exchange=str, routing_key=str, queue=str,
          options=record, passive=bool, durable=bool, exclusive=bool,
          no_auto_delete=bool, no_local=bool, ack=bool]
```

## Description

The `load_amqp` operator is an [AMQP](https://www.amqp.org/) 0-9-1 client that
enables interacting with an AMQP server, as a *consumer*.

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
and immediately dequeues it. This functionality may decrease performance at
and improve reliability. Without this flag, messages can get lost if a client
dies before they are delivered to the application.

Defaults to `false`.

## Examples

Consume [JSON](read_json.md) from a specific AMQP server:

```tql
load_amqp "amqp://admin:pass@0.0.0.1:5672/vhost"
read_json
```
