# pcap

Reads and writes raw network packets in libpcap format.

## Synopsis

Parser:

```
pcap [--disable-community-id] 
     [-c|--cutoff=<uint64t>] [-m|--max-flows=<uint64t>]
     [-a|--max-flow-age=<duration>] [-e|--flow-expiry=<uint64t>]
```

Printer:

```
pcap
```

## Description

The `pcap` parser converts raw bytes representing libpcap packets into events,
and the `pcap` printer generates libpcap streams from events.

The format abstracts away the concrete packet representation. Currently, the
only supported representation is [PCAP](http://www.tcpdump.org). The concrete
representation is an attribute to the `payload` field in the packet schema
outlined below:

```yaml
vast.packet:
  record:
    - time:
        timestamp: time
    - src: ip
    - dst: ip
    - sport:
        port: uint64
    - dport:
        port: uint64
    - vlan:
        record:
          - outer: uint64
          - inner: uint64
    - community_id:
        type: string
        attributes:
          index: hash
    - payload:
        type: string
        attributes:
          format: pcap
          skip: ~
```

The default loader for the `pcap` parser is [`stdin`](../connectors/stdin.md).

The default saver for the `pcap` printer is [`stdout`](../connectors/stdout.md).

### VLAN Tags

While decapsulating packets, VAST extracts
[802.1Q](https://en.wikipedia.org/wiki/IEEE_802.1Q) VLAN tags into the nested
`vlan` record, consisting of an `outer` and `inner` field for the respective
tags. The value of the VLAN tag corresponds to the 12-bit VLAN identifier (VID).
Special values include `0` (frame does not carry a VLAN ID) and `0xFFF`
(reserved value; sometimes wildcard match).

### Flow Management

The `pcap` format has a few tuning knows for controlling the amount of data
to keep per flow. Naive approaches, such as sampling or using a "snapshot"
(`tcpdump -s`) make analysis that requires flow reassembly impractical due to
incomplete byte streams. Inspired by the [Time Machine][tm], the `pcap`
format supports recording only the first *N* bytes of a connection (the
*cutoff*) and skipping the bulk of the flow data. This allows for recording most
connections in their entirety while achieving a large space reduction by
forgoing the heavy tail of the traffic distribution.

[tm]: http://www.icir.org/vern/papers/time-machine-sigcomm08.pdf

In addition to cutoff configuration, the `pcap` parser has other knobs
to tune management of the flow table managmenet that keeps the per-flow state.
The `--max-flows`/`-m` option specifies an upper bound on the flow table size in
number of connections. After a certain amount of inactivity of a flow,
the corresponding state expires. The option `--max-flow-age`/`-a` controls this
timeout value. Finally, the frequency of when the flow table expires entries
can be controlled via `--flow-expiry`/`-e`.

### `--disable-community-id`

Disables computation of the per-packet [Community
ID](https://github.com/corelight/community-id-spec).

By default, VAST populates the `community_id` field in the packet schema with a
string representation of the Community ID, e.g.,
`1:wCb3OG7yAFWelaUydu0D+125CLM=`. Use `--disable-community-id` to disable
computation of the Community ID, e.g., to save resources.

### `-c|--cutoff=<uint64>`

Sets the cutoff in number of bytes per TCP byte stream.

For example, to only record the first 1,024 bytes of every connection, pass
`--cutoff=1024` as option to the `pcap` parser. Note that the cut-off is
*bi-directional*, i.e., it applies to both the originator and responder TCP
streams and flows get evicted only after both sides have reached their cutoff
value.

### `-m|--max-flows=<int>`

Sets an upper bound on the flow table size where `<int>` represents the maximum
number of flows.

After the flow table exceeds its maximum size, the parser evicts random flows to
make room for the new flows. Increasing this value may reduce such evictions, at
the cost of a higher memory footprint.

Defaults to 1,048,576 (1Mi).

### `-a|--max-flow-age=<duration>`

The flow table removes entries after a certain amount of inactivity, i.e., when
a given flow no longer sees any new packets.

Increasing this value keeps flows for a longer period of time in the flow table,
which can benefit when computing the flow cutoff, but comes at a cost of a
higher memory footprint.

Defaults to 60 seconds.

### `-e|--flow-expiry=<duration>`

Controls the frequency of flow table expirations.

Increasing this value reduces pressure on the flow table, at the cost of
a potentially larger memory footprint.

Defaults to 10 seconds.

## Examples

Read packets from a PCAP file:

```
from file /tmp/trace.pcap read pcap
```

Filter a PCAP trace with VAST:

```bash
tcpdump -r trace.pcap -w - |
  vast exec 'read pcap | where vast.packet.time > 5 mins ago | write pcap' |
  tcpdump -r - -nl
```

Extract packets as JSON that have the address 6.6.6.6 as source or destination,
and destination port 5158:

```
read pcap | where 6.6.6.6 && dport == 5158 | write json
```

Query VLAN IDs using `vlan.outer` and `vlan.inner`:

```bash
read pcap | where vlan.outer > 0 || vlan.inner in [1, 2, 3]
```

Filter packets by Community ID:

```
read pcap 
| where community_id == "1:wCb3OG7yAFWelaUydu0D+125CLM=" 
| write pcap
```
