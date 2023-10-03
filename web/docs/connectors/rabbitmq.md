# rabbitmq

Sends and receives messages via RabbitMQ.

## Synopsis

Loader:

```
rabbitmq [-c|--channel <number>] [-q|--queue <queue>] [-X|--set <key=value>,...]
         [--passive] [--durable] [--exclusive] [--no-auto-delete] [--no-local]
         [--ack]
         [<url>]
```

Saver:

```
rabbitmq [-c|--channel <number>] [-q|--queue <queue>] [-X|--set <key=value>,...]
         [--mandatory] [--immediate]
         [<url>]
```

## Description

The `rabbitmq` connector enables interacting with a
[RabbitMQ](https://www.rabbitmq.com/) server. The loader acts as *consumer* and
the saver as *producer*.

RabbitMQ supports multiple protocols, the currently implementation can only
speak [AMQP](https://www.amqp.org/).

### `-c|--channel <number>` (Loader, Saver)

The channel number to use.

Defaults to `1`.

### `-e|--exchange <string>` (Loader, Saver)

The exchange to interact with.

Defaults to `amq.direct`.

### `-q|--queue <queue>` (Loader, Saver)

The name of the queue to use.

Defaults to `tenzir`.

### `-X|--set <key=value>` (Loader, Saver)

A comma-separated list of key-value configuration options for RabbitMQ.
`-X max_channels=42,frame_size=1024,sasl_method=external`. The example
`rabbitmq.yaml` file below shows the available options:

import CodeBlock from '@theme/CodeBlock';
import Configuration from '!!raw-loader!@site/../plugins/rabbitmq/rabbitmq.yaml.example';

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
The corresponding RabbitMQ server flag is called `auto-delete`. But since this
defaults to `true`, you can only specify the inverse via `--no-auto-delete`.
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
The corresponding RabbitMQ server flag is called `no-ack`. But since
this defaults to `true`, you can only specify the inverse via `--ack`.
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
from rabbitmq amqp://admin:pass:@0.0.0.1:5672/vhost
```

Send the list of all TQL operators to the `tenzir` exchange:

```
show operators | to rabbitmq --exchange tenzir
```
