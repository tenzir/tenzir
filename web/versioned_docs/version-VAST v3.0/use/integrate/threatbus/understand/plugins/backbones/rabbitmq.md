# RabbitMQ Backbone Plugin

The [RabbitMQ](https://www.rabbitmq.com) plugin enables Threat Bus to use
RabbitMQ as message broker backbone. RabbitMQ provides a reliable,
high-performance message passing infrastructure for indicator delivery within
Threat Bus. Using this backbone plugin, Threat Bus relays all messages through a
RabbitMQ endpoint. As a result, Threat Bus can scale horizontally via RabbitMQ.

This plugin simplifies network segregation and the communication trust model.
Threat Bus requires no trust between the connected applications. Connected apps
only need to know one Threat Bus endpoint, while Threat Bus itself only needs
to know a RabbitMQ endpoint.

The plugin implements the minimal
[backbone specs](https://github.com/tenzir/threatbus/blob/master/threatbus/backbonespecs.py)
for Threat Bus backbone plugins.

## Installation

Install the RabbitMQ backbone plugin via `pip`.

```bash
pip install threatbus-rabbitmq
```

## Configuration

The plugin requires some configuration parameters, as described in the example
excerpt from a Threat Bus `config.yaml` file below.

```yaml
...
plugins:
  backbones:
    rabbitmq:
      host: localhost
      port: 5672
      username: guest
      password: guest
      vhost: /
      exchange_name: threatbus
      queue:
        name_suffix: "my_suffix" # this defaults to 'hostname' if left blank
        name_join_symbol: . # queue will be named "threatbus" + join_symbol + name_suffix
        durable: true
        auto_delete: false
        lazy: true
        exclusive: false
        max_items: 100000 # optional. remove property / set to 0 to allow infinite length
...
```

### Parameter Explanation

While most parameters are self-explanatory, like `host` and `port`, others
require some further explanation as described below.

##### queue.name_suffix

Each Threat Bus host binds its own queue to the RabbitMQ exchange. The name of
that queue should not overlap with the queue names from other Threat Bus
instances (i.e., in a shared RabbitMQ host scenario). Hence, queue names are by
default suffixed with the `hostname` of the Threat Bus instance that binds to
them. Use the option `queue.name_suffix` to override the name-suffix of queues
with a user-specified value, instead of the hostname.

#### queue.name_join_symbol

The plugin creates a
[fanout exchange](https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-fanout)
and binds a queue to it on the RabbitMQ host. The option `name_join_symbol`
provides some flexibility to the user when it comes to the naming of these
resources.

For example, if your organization has a naming scheme to always concatenate
resource names based on their domain via `_`, you can specify that here. The
resulting queue name with then be e.g., `threatbus_<queue.name_suffix>`.

#### queue.durable

Sets the [queue property](https://www.rabbitmq.com/queues.html#properties) for
durable queues. If `true`, queues will survive RabbitMQ broker restarts.

#### queue.auto_delete

Sets the [queue property](https://www.rabbitmq.com/queues.html#properties) to
auto-delete queues. If `true`, these queues will be cleared from the RabbitMQ
host when Threat Bus disconnects.

#### queue.lazy

If set, Threat Bus will declare all queues as
[lazy](https://www.rabbitmq.com/lazy-queues.html). If `true`, RabbitMQ shifts
queue contents to disk early and optimizes for memory management.

#### queue.exclusive

Sets the [queue property](https://www.rabbitmq.com/queues.html#properties) for
exclusive queues. If `true`, a RabbitMQ queue can only be used by one connection
and is deleted after that connection closes.

#### queue.max_items

Limits the [maximum amount](https://www.rabbitmq.com/maxlength.html) of items in
a RabbitMQ queue.
