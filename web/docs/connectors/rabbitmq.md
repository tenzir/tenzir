# rabbitmq

Sends and receives messages via RabbitMQ.

## Synopsis

```
rabbitmq [-c|--channel <number>] [-q|--queue <queue>] [-X|--set <key=value>,...]
```

## Description

The `rabbitmq` connector enables interacting with a
[RabbitMQ](https://www.rabbitmq.com/) server. The loader acts as *consumer* and
the saver as *producer*.

RabbitMQ supports multiple protocols, the currently implementation can only
speak [AMQP](https://www.amqp.org/).

### `-c|--channel <number>`

The channel number to use.

Defaults to `1`.

### `-e|--exchange <string>`

The exchange to interact with.

Defaults to `amq.direct`.

### `-q|--queue <queue>`

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

## Examples

Consume [JSON](../formats/json.md) from the queue `foo`:

```
from rabbitmq --queue foo read json
```

Send the list of all TQL operators to the `tenzir` exchange:

```
show operators | to rabbitmq --exchange tenzir
```
