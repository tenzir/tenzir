# AMQP

The [Advanced Message Queuing Protocol (AMQP)](https://www.amqp.org/) is an open
standard application layer protocol for message-oriented middleware.

The diagram below shows the key abstractions and how they relate to a pipeline:

![AMQP Diagram](amqp.svg)

Tenzir supports sending and receiving messages via AMQP version 0-9-1.

The URL scheme `amqp://` dispatches to
[`load_amqp`](../../tql2/operators/load_amqp.md) and
[`save_amqp`](../../tql2/operators/save_amqp.md) for seamless URL-style use via
[`from`](../../tql2/operators/from.md) and [`to`](../../tql2/operators/to.md)

## Send events to an AMQP exchange

```tql
from {
  x: 42,
  y: "foo",
}
to "amqp://admin:pass@0.0.0.1:5672/vhost"
```

## Receive events from an AMQP queue

```tql
from "amqp://admin:pass@0.0.0.1:5672/vhost"
```
