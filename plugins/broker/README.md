# Broker Plugin for VAST

The Broker plugin for VAST adds the ability to import and export data in the
Broker format to VAST.

## Import

The `import broker` command ingests events via Zeek's Broker.

Broker provides a topic-based publish-subscribe communication layer and
standardized data model to interact with the Zeek ecosystem. Using the `broker`
reader, VAST can transparently establish a connection to Zeek and subscribe log
events. Letting Zeek send events directly to VAST cuts out the operational
hassles of going through file-based logs.

To connect to a Zeek instance, run the `import broker` command without
arguments:

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

By default, VAST automatically subscribes to the topic `zeek/logs/` because this
is where Zeek publishes log events. Use `--topic` to set a different topic.

## Export

The `export broker` command sends query results to Zeek via Broker.

Broker provides a topic-based publish-subscribe communication layer and
standardized data model to interact with the Zeek ecosystem. Using the `broker`
writer, VAST can send query results to a Zeek instance. This allows you to write
Zeek scripts incorporate knowledge from the past that is no longer in Zeek
memory, e.g., when writing detectors for longitudinal attacks.

To export a query into a Zeek instance, run the `export broker` command:

```bash
# Spawn a Broker endpoint, connect to localhost:9999/tcp, and publish
# to the topic `vast/data` to send result events to Zeek.
vast export broker <expression>
```

To handle the data in Zeek, your script must write a handler for the following event:

```zeek
event VAST::data(layout: string, data: any)
  {
  print layout, data; // dispatch
  }
```

The event argument `layout` is the name of the event in the VAST table slice.
The `data` argument a vector of Broker data values reprsenting the event.

By default, VAST automatically publishes a Zeek event `VAST::data` to the topic
`vast/data/`. Use `--event` and `--topic` to set these options to different
values.
