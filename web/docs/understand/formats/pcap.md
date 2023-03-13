---
description: Raw network traffic in PCAP form
---

# PCAP

VAST supports reading and writing [PCAP](http://www.tcpdump.org) traces via
`libpcap`.

## Parser

On the read path, VAST can either acquire packets from a trace file or in *live
mode* from a network interface.

While decapsulating packets, VAST extracts
[802.1Q](https://en.wikipedia.org/wiki/IEEE_802.1Q) VLAN tags into the nested
`vlan` record, consisting of an `outer` and `inner` field for the respective
tags. The value of the VLAN tag corresponds to the 12-bit VLAN identifier (VID).
Special values include `0` (frame does not carry a VLAN ID) and `0xFFF`
(reserved value; sometimes wildcard match).

In addition, VAST computes the [Community
ID](https://github.com/corelight/community-id-spec) per packet to support
pivoting from other log data. The packet record contains a field `community_id`
that represents the string representation of the Community ID, e.g.,
`1:wCb3OG7yAFWelaUydu0D+125CLM=`. If you prefer to not have the Community ID in
your data, add the option `--disable-community-id` to the `pcap` command.

To ingest a PCAP file `input.trace`, pass it to the `pcap` command on standard
input:

```bash
vast import pcap < input.trace
```

You can also acquire packets by listening on an interface:

```bash
vast import pcap -i eth0
```

### Real-World Traffic Replay

When reading PCAP data from a trace, VAST processes packets directly one after
another. This differs from live packet capturing where there exists natural
inter-packet arrival times, according to the network traffic pattern. To emulate
"real-world" trace replay, VAST supports a *pseudo-realtime* mode, which works
by introducing inter-packet delays according to the difference between subsquent
packet timestamps.

The option `--pseudo-realtime`/`-p` takes a positive integer *c* to delay
packets by a factor of *1/c*. For example, if the first packet arrives at time
*t0* and the next packet at time *t1*, then VAST would sleep for time
*(t1 - t0)/c* before releasing the second packet. Intuitively, the larger *c*
gets, the faster the replay takes place.

For example, to replay packets as if they arrived in realtime, use `-p 1`. To
replay packets twice as fast as they arrived on the NIC, use `-p 2`.

### Flow Management

The PCAP plugin has a few tuning knows for controlling storage of connection
data. Naive approaches, such as sampling or using a "snapshot" (`tcpdump -s`)
make transport-level analysis impractical due to an incomplete byte stream.
Inspired by the [Time Machine][tm], the PCAP plugin supports recording only the
first *N* bytes of a connection (the *cutoff*) and skipping the bulk of the flow
data. This allows for recording most connections in their entirety while
achieving a massive space reduction by forgoing the heavy tail of the traffic
distribution.

[tm]: http://www.icir.org/vern/papers/time-machine-sigcomm08.pdf

To record only the first 1,024 bytes every connection, pass `-c 1024` as option.
Not that the cut-off is *bi-directional*, i.e., it applies to both the
originator and responder TCP streams and a flow gets evicted only after both
sides have reached their cutoff value.

In addition to cutoff configuration, the PCAP plugin has a few other tuning
parameters. VAST keeps a flow table with per-connection state. The
`--max-flows`/`-m` option specifies an upper bound on the flow table size in
number of connections. After a certain amount of inactivity of a flow,
the corresponding state expires. The option `--max-flow-age`/`-a` controls this
timeout value. Finally, the frequency of when the flow table expires entries
can be controlled via `--flow-expiry`/`-e`.

## Printer

On the write path, VAST can write packets to a trace file.

:::info Writing PCAP traces
VAST can only write PCAP traces for events of type `pcap.packet`. To avoid
bogus trace file files, VAST automatically appends `#type == "pcap.packet"` to
every query expression.
:::

Below are some examples queries the generate PCAP traces. In principle, you can
also use other output formats aside from `pcap`. These will render the binary
PCAP packet representation in the `payload` field.

### Extract packets in a specific time range

VAST uses the timestamp from the PCAP header to determine the event time for a
given packet. To query all packets from the last 5 minutes, leverage the `time`
field:

```bash
vast export pcap 'pcap.packet.time > 5 mins ago' | tcpdump -r - -nl
```

### Extract packets matching IPs and ports

To extract packets matching a combination of the connection 4-tuple, you can
use the `src`, `dst`, `sport`, and `dport` fields. For example:

```bash
vast export pcap '6.6.6.6 && dport == 5158' | tcpdump -r - -nl
```

### Extract packets matching VLAN IDs

VAST extracts outer and inner VLAN IDs from 802.1Q headers. You can query VLAN
IDs using `vlan.outer` and `vlan.inner`:

```bash
vast export pcap 'vlan.outer > 0 || vlan.inner in [1, 2, 3]' | tcpdump -r - -nl
```

Special IDs include `0x000` (frame does not carry a VLAN ID) and `0xFFF`
(reserved value; sometimes wildcard match). If you would like to check the
presence of a header, check whether it is null, e.g., `vlan.outer != null`.

### Extract packet matching a Community ID

Use the `community_id` field to query all packets belonging to a single flow
identified by a Community ID:

```bash
vast export pcap 'community_id == "1:wCb3OG7yAFWelaUydu0D+125CLM="' |
  tcpdump -r - -nl
```
