---
sidebar_position: 1
---

# Subscriptions and Message Passing

This section explains how Threat Bus manages subscriptions and describes the
message passing flow between subscribers. The document addresses developers who
are interested in contributing to Threat Bus or otherwise want to learn about
the runtime internals.

## Separation of Concerns

Threat Bus and all plugins are implemented with python
[threads](https://docs.python.org/3/library/threading.html) and thread-safe,
synchronized [queues](https://docs.python.org/3/library/queue.html). The main
loop of Threat Bus must never be blocked. All plugins should implement the
[StoppableWorker](https://github.com/tenzir/threatbus/blob/master/threatbus/stoppable_worker.py)
base class to model busy work. Implementing that class also facilitates a
graceful shutdown.

As plugins run in their own threads and are not aware of each other, Threat Bus
uses queues to enable communication between them. On start-up, Threat Bus
creates one **global queue for incoming messages**. Let's call it `inq`. Threat
Bus passes a reference to this queue to all installed plugins &mdash; backbone
and application plugins alike. Per convention, only application plugins write to
the `inq`, while backbone plugins consume messages from it.

## Subscription Flow

Threat Bus provides two callbacks to all application plugins for subscribing and
unsubscribing apps. You can find their implementation in the
[`threatbus.py`](https://github.com/tenzir/threatbus/blob/master/threatbus/threatbus.py)
entry point of the Threat Bus application. The signature of the callbacks looks
as follows.

```py
subscribe(topic: str, q: JoinableQueue, time_delta: timedelta = None)
unsubscribe(topic: str, q: JoinableQueue)
```

Applications (e.g., a Zeek instance) un/subscribe from/to Threat Bus via an
application plugin (e.g., `threatbus-zeek` offers a `broker` endpoint for Zeek
instances to connect with). All communication between application and app plugin
uses an **app-specific** message format. In our example, Zeek sends `broker`
messages.

The application plugin transforms received messages from the app-specific
message format and invokes the corresponding callback for un/subscribing.

:::note
This message format mapping is the same for all kinds of messages exchanged
between apps and app plugins, be it un/subscriptions or security content.
:::

### Subscriptions

Subscribing requires passing a topic and an optional integer for requesting a
[snapshot](snapshotting) of historic indicators. Application plugins create a
new **queue for every subscription** they receive from an app. Let's call that
queue `outq_1`.

Backbones provision incoming messages from the global `inq` to all subscribers
(all the many `outq_n`s). But how do backbones become aware of new queues?

This is done via the `subscribe` callback. Application plugins pass the
subscribed topic along with the newly created `outq_x` to Threat Bus. Threat Bus
then instructs all registered backbones to provision messages for the
requested topic to the new queue (`outq_1` in our example).

Once subscribed, application plugins read from the `outq`s they created. The
plugins are responsible to forward all messages that appear in any given `outq`
to the subscribed app. How that is done, for example over the wire, is
implementation specific logic and handled by the plugin (e.g., via `broker` to a
Zeek instance or via ZeroMQ to VAST).

### Unsubscriptions

Unsubscription works just as subscription, via a callback to Threat Bus. A
subscribed app, e.g., a Zeek instance, unsubscribes at the responsible app
plugin using the app-specific format (`broker` in this case). The plugin parses
the request, extracts the topic the app wishes to unsubscribe from, and forwards
that topic along with the corresponding `outq_x` of the subscriber to Threat Bus
via the `unsubscribe()` callback shown above. Threat Bus then instructs all
backbones to forget about the said `outq_x`.

## Message Passing

This section outlines how messages flow through Threat Bus on the example of two
already subscribed applications -- the
[OpenCTI connector](https://github.com/OpenCTI-Platform/connectors/tree/master/threatbus)
and [Zeek](https://zeek.org/).

In our example, Threat Bus is equipped with three plugins:

- `threatbus-zeek` for communicating with Zeek instances via `broker`.
- `threatbus-zmq` for communicating via ZeroMQ (i.e., with the
  `opencti-connector`).
- `threatbus-inmem` for having a simple, in-memory backbone.

A Zeek instance has already subscribed to Threat Bus via the `threatbus-zeek`
plugin's `broker` endpoint. It is subscribed to the topic `stix2/indicator` and
an appropriate `outq` is already created (see the [Subscription
Flow](#subscription-flow) above).

Let's assume the `opencti-connector` sends a STIX-2 indicator to Threat Bus via
ZeroMQ. The message arrives at the `threatbus-zmq` plugin. Format conversion
is not required, because the message is already in STIX-2 format. The plugin now
puts this message in the global `inq`.

In another thread, the `threatbus-inmem` plugin continuously reads from the
`inq`. It is also aware of the subscription from the Zeek instance for the topic
`stix2/indicator`. Because the incoming message is of exactly that type, the
backbone clones the message from the `inq` and puts it into the `outq`.

The `threatbus-zeek` plugin (again in another thread) continuously monitors all
`outq`s of its subscribed Zeek instances. Once the new message arrives in the
`outq` `threatbus-zeek` maps the STIX-2 indicator to a `broker` compatible
format before sending it out to the appropriate Zeek instance.

Finally, the Zeek instance receives the message and can ingest it into its intel
framework. Should Zeek generate a sighting now, the message would similarly flow
all the way back into OpenCTI, just reversing the flow.
