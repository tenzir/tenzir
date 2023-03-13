# Plugins

Threat Bus has a plugin-based architecture, implemented via
[pluggy](https://pluggy.readthedocs.io/). The following document describes the
existing plugin types in detail and explains their responsibilities.

## Plugin Types

Threat Bus differentiates two plugin types. **Application plugins** extend
Threat Bus to provide a new endpoint for "apps" to connect with. How the
endpoint looks like, and what message-formats are used for communication is up
to the application plugin. "Apps" are security tools, like Zeek or VAST.

**Backbone plugins** are required for actual message
passing. At least one backbone plugin is required for the bus to
be functional. The most basic plugin for that job is the
[in-memory backbone](plugins/backbones/in-mem).

### Application Plugins

Application plugins are the entrypoint for applications to connect with Threat
Bus. App plugins do two things:

1. **They provide an endpoint.**
  App plugins provide an endpoint (i.e., they bind certain ports) and offer a
  communication protocol. For example, the
  [Zeek plugin](plugins/apps/zeek) exposes a
  [Broker](https://github.com/zeek/broker) endpoint. Broker is the native
  communication protocol for Zeek ("the app"), so it is the Zeek plugin's
  responsibility to expose an endpoint that Zeek instances can work with.
2. **They handle message-format mapping.**
  Threat Bus internally uses the
  [STIX-2](https://oasis-open.github.io/cti-documentation/stix/intro.html)
  serialization format. Not all open-source security tools can work
  with this format. App plugins can implement mappings from STIX-2 to other
  formats, so that connecting apps can work with it. For example, the MISP
  plugin receives a proprietary format from MISP and transforms these attribute
  to STIX-2 Indicators.

Threat Bus itself is at no point concerned with alternative formats, nor with
details about the connected applications, nor exposed endpoints.
Instead, Threat Bus is only aware of `Subscribers` and STIX-2 messages that are
exchanged between them.

### Backbone Plugins

Backbone plugins either implement the message provisioning logic themselves
(like the [in-memory backbone plugin](plugins/backbones/in-mem)) or
offer a transparent interface to existing message brokers, like the
[RabbitMQ backbone plugin](plugins/backbones/rabbitmq) does.

Threat Bus notifies all backbone-plugins whenever new subscriptions come in.
A subscription consists of a `topic` and a queue (`outq`) that is used by the
subscriber to receive messages. The job of a backbone plugin is to read messages
from the global queue of incoming messages (`inq`), optionally hand these over
to an existing messaging broker (like RabbitMQ) and consume them back, and
lastly, based on the topic, sort those messages to all subscribed `outq`s.

## Apps and Wrappers

Threat Bus is a pub/sub broker, and hence applications have to subscribe
themselves to the bus. However, that requires active behavior from applications,
and hence could require changes to the source code. Generally speaking, that is
undesirable.

Some applications, like Zeek, can be scripted. In that particular case, the
logic for connecting to Threat Bus is implemented in a separate
[script](https://github.com/tenzir/threatbus/tree/master/apps/zeek).

Other applications, like VAST, cannot be scripted. Those applications require
either a change to the source code or a wrapper script to initiate communication
with Threat Bus. See
[VAST Threat Bus](https://pypi.org/project/vast-threatbus/) for an example
application.
