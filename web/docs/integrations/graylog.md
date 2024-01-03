# Graylog

[Graylog](https://graylog.org/) is a log management solution based on top of
Elasticsearch.

Use Tenzir to process data from Graylog.

In Graylog, data goes through three key stages:

1. **Inputs**: This stage involves data ingestion, where Graylog receives data
   from various sources. Inputs support multiple protocols like TCP, UDP, and
   HTTP. They normalize incoming data into the unified [Graylog Extended Log
   Format (GELF)][gelf].
2. **Streams**: Once onboarded, streams route the data internally. Here,
   it can be filtered, parsed, and enriched. The stream processing stage
   leverages extractors and pipeline rules for data manipulation, before
   indexing the data in Elasticsearch for storage.
3. **Outputs**: For exporting data, Graylog utilizes alerts, dashboards, and its
   REST API. Additionally, Graylog can forward data to external systems or tools
   via streams.

[gelf]: ../formats/gelf.md

## Receive data from Graylog

To receive data from Graylog with a Tenzir pipeline, you need to configure a new
output and setup a stream that sends data to that output. The example below
assumes that Graylog sends data in GELF to a TCP endpoint that listens on
1.2.3.4 at port 5678.

### Configure a GELF TCP output

1. Navigate to *System/Outputs* in Graylog's web interface.
2. Click *Manage Outputs*.
3. Select `GELF TCP` as the output type.
4. Configure the output settings:
   - Specify the target server's address in the `host` field (e.g., 1.2.3.4).
   - Enter the port number for the TCP connection (e.g., 5678).
   - Optionally adjust other settings like reconnect delay, queue size, and send
     buffer size.
5. Save the configuration.

Now Graylog will forward messages in GELF format to the specified TCP endpoint.

### Create a Graylog stream

The newly created output still needs to be connected to a stream to produce
data. For example, to route all incoming traffic in Graylog to an output:

1. Go to *Streams* in the Graylog web interface.
2. Create a new stream or edit an existing one.
3. In the stream's settings, configure it to match all incoming messages. You
   can do this by setting up a rule that matches all messages or by leaving the
   rules empty.
4. Once the stream is configured, go to the *Outputs* tab in the stream's
   settings.
5. Add the previously configured GELF TCP output to this stream.

This setup will direct all messages that arrive in Graylog to the specified
output. Adapt your filters for more fine-grained forwarding.

### Test the connection with a Tenzir pipeline

Now that Graylog is configured, you can test that data is flowing using the
following Tenzir pipeline:

```
from tcp://1.2.3.4:5678 read gelf
```

This pipelines opens a listening socket at IP address 1.2.3.4 at port 5678 via
the [tcp](../connectors/tcp.md) loader, and then reads a stream of GELF messages
using the [gelf](../formats/gelf.md) parser. Graylog will connect to this
socket, based on the reconnect interval that you configured in the output (by
default 500ms).

Now that data is flowing, you can decide what to do with the Graylog data, e.g.,
ingest data into a running Tenzir node by appending
[import](../operators/import.md):

```
from tcp://1.2.3.4:5678 read gelf
| import
```
