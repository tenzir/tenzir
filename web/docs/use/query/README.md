# Query

import MissingDocumentation from '@site/presets/MissingDocumentation.md';

<MissingDocumentation/>

## Choose an export format

<MissingDocumentation/>

### Zeek

<MissingDocumentation/>

#### Broker

The `broker` export command sends query results to Zeek via the
[Broker](https://github.com/zeek/broker) communication library.

Broker provides a topic-based publish-subscribe communication layer and
standardized data model to interact with the Zeek ecosystem. Using the `broker`
writer, VAST can send query results to a Zeek instance. This allows you to write
Zeek scripts incorporate knowledge from the past that is no longer in Zeek
memory, e.g., when writing detectors for longitudinal attacks.

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

### Arrow

VAST supports [reading](/docs/use/ingest#arrow) and writing  data in the
[`Arrow IPC`](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
columnar format, suitable for efficient handling of large data sets. It is
used by VAST's python bindings to efficiently transfer data to be used with
data analysis tools like [pandas](https://pandas.pydata.org/).
Since Arrow IPC is self-contained and includes the full schema, it can be
used to transfer data between VAST nodes, even if the target node is not aware
of the underlying schema.

:::note
VAST makes use of Arrow
[extension types](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
to properly describe domain-specific domain-specific concepts like ip addresses
or subnets. Our python bindings come with the required tooling, so you can
work with native types instead of relying on error-prone string representations.
:::

### PCAP

VAST supports [reading](/docs/use/ingest#pcap) and writing
[PCAP](http://www.tcpdump.org) traces via `libpcap`. On the write path, VAST can
write packets to a trace file.

:::info Writing PCAP traces
VAST can only write PCAP traces for events of type `pcap.packet`. To avoid
bogus trace file files, VAST automatically appends `#type == "pcap.packet"` to
every query expression.
:::

Below are some examples queries the generate PCAP traces. In principle, you can
also use other output formats aside from `pcap`. These will render the binary
PCAP packet representation in the `payload` field.

#### Extract packets in a specific time range

VAST uses the timestamp from the PCAP header to determine the event time for a
given packet. To query all packets from the last 5 minutes, leverage the `time`
field:

```bash
vast export pcap 'pcap.packet.time > 5 mins ago' | tcpdump -r - -nl
```

#### Extract packets matching IPs and ports

To extract packets matching a combination of the connection 4-tuple, you can
use the `src`, `dst`, `sport`, and `dport` fields. For example:

```bash
vast export pcap '6.6.6.6 && dport == 42000' | tcpdump -r - -nl
```

#### Extract packets matching VLAN IDs

VAST extracts outer and inner VLAN IDs from 802.1Q headers. You can query VLAN
IDs using `vlan.outer` and `vlan.inner`:

```bash
vast export pcap 'vlan.outer > 0 || vlan.inner in [1, 2, 3]' | tcpdump -r - -nl
```

Special IDs include `0x000` (frame does not carry a VLAN ID) and `0xFFF`
(reserved value; sometimes wildcard match). If you would like to check the
presence of a header, check whether it null, e.g., `vlan.outer != nil`.

#### Extract packet matching a Community ID

Use the `community_id` field to query all packets belonging to a single flow
identified by a Community ID:

```bash
vast export pcap 'community_id == "1:wCb3OG7yAFWelaUydu0D+125CLM="' |
  tcpdump -r - -nl
```
