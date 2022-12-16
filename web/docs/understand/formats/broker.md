---
description: Zeek-native publish/subscribe
---

# Broker

[Broker](https://github.com/zeek/broker) provides a topic-based
publish-subscribe communication layer and standardized data model to interact
with the Zeek ecosystem.

:::warning Plugin Removed
We [removed](https://github.com/tenzir/vast/pull/2796) the `broker` plugin
because it only worked with Zeek v4.x, but no longer with the later Zeek v5.x
series. We will bring back the plugin after we have upgraded VAST to CAF v0.19.
Please consider the existing documentation only as a reference of functionality
to expect.
:::

## Input

The `broker` import command ingests events via Zeek's communication library.
Letting Zeek send events directly to VAST cuts out the operational hassles of
going through file-based logs.

This works by transparently establishing a connection to a Zeek node and
subscribing to log events. To connect to a Zeek instance, run the `broker`
command without arguments:

```bash
# Spawn a Broker endpoint, connect to localhost:9999/tcp, and subscribe
# to the topic `zeek/logs/` to acquire Zeek logs.
vast import broker
```

Logs should now flow from Zeek to VAST, assuming that Zeek has the following
default settings:

- The script variable `Broker::default_listen_address` is set to `127.0.0.1`.
  Zeek populates this variable with the value from the environment variable
  `ZEEK_DEFAULT_LISTEN_ADDRESS`, which defaults to `127.0.0.1`.
- The script variable `Broker::default_port` is set to `9999/tcp`.
- The script variable `Log::enable_remote_logging` is set to `T`.

Note: you can spawn Zeek with `Log::enable_local_logging=F` to avoid writing
additional local log files.

You can also spawn a Broker endpoint that is listening instead of connecting:

```bash
# Spawn a Broker endpoint, listen on localhost:8888/tcp, and subscribe
# to the topic `foo/bar`.
vast import broker --listen --port=8888 --topic=foo/bar
```

By default, VAST automatically subscribes to the topic `zeek/logs/` because
this is where Zeek publishes log events. Use `--topic` to set a different topic.

## Output

The `broker` export command sends query results to Zeek via the
[Broker](https://github.com/zeek/broker) communication library.

This allows you to write Zeek scripts incorporate knowledge from the past that
is no longer in Zeek memory, e.g., when writing detectors for longitudinal
attacks.

To export a query into a Zeek instance, run the `broker` command:

```bash
# Spawn a Broker endpoint, connect to localhost:9999/tcp, and publish
# to the topic `vast/data` to send result events to Zeek.
vast export broker <expression>
```

To handle the data in Zeek, your script must write a handler for the following
event:

```zeek
event VAST::data(layout: string, data: any)
  {
  print layout, data; // dispatch
  }
```

The event argument `layout` is the name of the event in the VAST table slice.
The `data` argument a vector of Broker data values representing the event.

By default, VAST automatically publishes a Zeek event `VAST::data` to the topic
`vast/data/`. Use `--event` and `--topic` to set these options to different
values.
