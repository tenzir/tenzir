---
title: Tenzir v4.2
authors: [Dakostu, mavam]
date: 2023-09-07
tags: [release, pipelines, connectors, s3, zmq]
draft: true
---

We've just released Tenzir v4.2 that introduces two new connectors: S3 for
interacting with blob storage and [ZeroMQ][zeromq] for writing distributed
multi-hop pipelines.

[zeromq]: https://zeromq.org/

<!--truncate-->

## S3 Saver & Loader

TODO

## ZeroMQ Saver & Loader

The new [`zmq`](/connectors/zmq) connector makes it easy to interact with the
raw bytes in [ZeroMQ][zeromq] messages. We model the `zmq` *loader* as subscriber with a `SUB` socket, and the *saver* as a publisher with the `PUB` socket:

![ZeroMQ Connector](zeromq-connector.excalidraw.svg)

What's nice about ZeroMQ is that the directionality of connection establishment
is independent of the socket type. So either end can bind or connect. We opted
for the subscriber to connect by default, and the publisher to bind. You can
override this with the `--bind` and `--connect` flags.

Even though we're using a lossy `PUB`/`SUB` socket pair, we've added a thin
layer of reliability in that a Tenzir pipeline won't send or receive ZeroMQ
messages before it has at least one connected socket.

Want to exchange and convert events with two single commands? Here's how you
publish JSON and continue as CSV on the other end:

```bash
# Publish some data via a ZeroMQ PUB socket:
tenzir 'show operators | to zmq write json'
# Subscribe to it in another process
tenzir 'from zmq read json | write csv'
```

You can also work with operators that use types. Want to send away chunks of
network packets to a remote machine? Here you go:

```bash
# Publish raw bytes:
tenzir 'load nic eth0 | save zmq'
# Tap into the raw feed at the other end and start parsing:
tenzir 'load zmq | read pcap | decapsulate'
```

Need to expose the source side of a pipeline as a listening instead of
connecting socket? No problem:

```bash
# Bind instead of connect with the ZeroMQ SUB socket:
tenzir 'from zmq --bind'
```

These examples show the power of composability: Tenzir operators can work with
bytes but also events, allowing for in-flight reshaping, format conversation, or
simply flexibling data shipping.

One very specific security use case that the ZeroMQ loader enables is attaching
to the [MISP ZeroMQ](https://www.circl.lu/doc/misp/misp-zmq/) firehose that
publishes events, attributes, sightings, and more. The ZeroMQ connector paved
the way for pipeline-native threat intelligence processing. Stay tuned for the
easy button to live and retro match indicators.
