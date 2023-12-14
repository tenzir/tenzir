---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# amqp

Sends and receives messages via AMQP.

## Synopsis

Loader:

```
amqp [-c|--channel <number>] [-e|--exchange <exchange>]
     [-r|--routing-key <key>] [-q|--queue <queue>]
     [-X|--set <key=value>,...] [--passive] [--durable] [--exclusive]
     [--no-auto-delete] [--no-local] [--ack] [<url>]
```

Saver:

```
amqp [-c|--channel <number>] [-e|--exchange <exchange>]
     [-r|--routing-key <key>] [-X|--set <key=value>,...] [--mandatory]
     [--immediate] [<url>]
```

## Description

The `amqp` connector is an [AMQP](https://www.amqp.org/) 0-9-1 client that
enables interacting with an AMQP server. The loader acts as *consumer* and the
saver as *producer*.

The diagram below shows the key abstractions and how they relate to a pipeline:

![AMQP](amqp.excalidraw.svg)

The implementation of this connector relies on the [RabbitMQ C client
library](https://github.com/alanxz/rabbitmq-c).

### `-c|--channel <number>` (Loader, Saver)

The channel number to use.

Defaults to `1`.

### `-e|--exchange <string>` (Loader, Saver)

The exchange to interact with.

Defaults to `amq.direct`.

### `-r|--routing-key <key>` (Loader, Saver)

For the loader, the name of the routing key to bind a queue to an exchange. For the saver, the routing key to publish messages with.

Defaults to the empty string.

### `-X|--set <key=value>` (Loader, Saver)

A comma-separated list of key-value configuration options for RabbitMQ, e.g.,
`-X max_channels=42,frame_size=1024,sasl_method=external`. The example
`amqp.yaml` file below shows the available options:

import CodeBlock from '@theme/CodeBlock';
import Configuration from '!!raw-loader!@site/../plugins/amqp/amqp.yaml.example';

<CodeBlock language="yaml">{Configuration}</CodeBlock>

We recommend factoring the environment-specific options into the configuration
file so that they are not cluttering the pipeline definition.

### `<url>` (Loader, Saver)

A URL that specifies the AMQP server. The URL must have the following format:

```
amqp://[USERNAME[:PASSWORD]@]HOSTNAME[:PORT]/[VHOST]
```

When the URL is present, it will overwrite the corresponding values of the
configuration options.

### `-q|--queue <queue>` (Loader)

The name of the queue to declare and then bind to.

Defaults to the empty string, resulting in auto-generated queue names, such as
`amq.gen-XNTLF0FwabIn9FFKKtQHzg`.

### `--passive` (Loader)

If set, the server will reply with OK if an exchange already exists with the
same name, and raise an error otherwise.

### `--durable` (Loader)

If set when creating a new exchange, the exchange will be marked as durable.
Durable exchanges remain active when a server restarts. Non-durable exchanges
(transient exchanges) are purged if/when a server restarts.

### `--exclusive` (Loader)

If set, marks the queue as exclusive. Exclusive queues may only be accessed by
the current connection, and are deleted when that connection closes. Passive
declaration of an exclusive queue by other connections are not allowed.

### `--no-auto-delete` (Loader)

If set, the exchange will *not* be deleted when all queues have finished using
it.

:::note Inverted Flag
The corresponding AMQP server flag is called `auto-delete`. Since we default to
`true` for this flag, you can disable it by specifying `--no-auto-delete`.
:::

### `--no-local` (Loader)

If set, the server will not send messages to the connection that published them.

### `--ack` (Loader)

If set, the server expects acknowledgements for messages. Otherwise, when a
message is delivered to the client the server assumes the delivery will succeed
and immediately dequeues it. This functionality may decrease performance at
and improve reliability. Without this flag, messages can get lost if a client
dies before they are delivered to the application.

:::note Inverted Flag
The corresponding AMQP server flag is called `no-ack`. Since we default to
`true` for this flag, you can enable it by specifying `--ack`.
:::

### `--mandatory` (Saver)

This flag tells the server how to react if the message cannot be routed to a
queue. If set, the server will return an unroutable message with a Return
method. Otherwise the server silently drops the message.

### `--immediate` (Saver)

This flag tells the server how to react if the message cannot be routed to a
queue consumer immediately. If set, the server will return an undeliverable
message with a Return method. If unset, the server will queue the message, but
with no guarantee that it will ever be consumed.

## Examples

Consume [JSON](../formats/json.md) from a specific AMQP server:

```
from amqp amqp://admin:pass@0.0.0.1:5672/vhost
```

Send the list of all TQL operators:

```
show operators | to amqp
```
