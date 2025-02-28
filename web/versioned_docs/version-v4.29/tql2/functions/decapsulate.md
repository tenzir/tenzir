# decapsulate

Decapsulates packet data at link, network, and transport layer.

```tql
decapsulate(packet:record) -> record
```

## Description

The `decapsulate` function decodes binary PCAP packet data by extracting link,
network, and transport layer information. The function takes a `packet` record
as argument as produced by the [PCAP parser](../../formats/pcap.md), which may
look like this:

```tql
{
  linktype: 1,
  timestamp: 2021-11-17T13:32:43.249525,
  captured_packet_length: 66,
  original_packet_length: 66,
  data: "ZJ7zvttmABY88f1tCABFAAA0LzBAAEAGRzjGR/dbgA6GqgBQ4HzXXzhE3N8/r4AQAfyWoQAAAQEICqMYaE9Mw7SY",
}
```

This entire record serves as input to `decapsulate` since the `linktype`
determines how to intepret the binary `data` field containing the raw packet
data.

:::note Wireshark?
With `decapsulate`, we aim to provide a *minimal* packet parsing up to the
transport layer so that you can work with packets in pipelines and implement use
cases such as alert-based PCAP. The goal is *not* to comprehensively parse all
protocol fields at great depth. If this is your objective, consider
[Zeek](https://zeek.org), [Suricata](https://suricata.io), or
[Wireshark](https://wireshark.org).
:::

### VLAN Tags

The `decapsulate` function also extracts
[802.1Q](https://en.wikipedia.org/wiki/IEEE_802.1Q) VLAN tags into a nested
`vlan` record, consisting of an `outer` and `inner` field for the respective
tags. The value of the VLAN tag corresponds to the 12-bit VLAN identifier (VID).
Special values include `0` (frame does not carry a VLAN ID) and `0xFFF`
(reserved value; sometimes wildcard match).

## Examples

### Decapsulate packets from a PCAP file

```tql
from "/path/to/trace.pcap"
this = decapsulate(this)
```

```tql
{
  ether: {
    src: "00-08-02-1C-47-AE",
    dst: "20-E5-2A-B6-93-F1",
    type: 2048,
  },
  ip: {
    src: 10.12.14.101,
    dst: 92.119.157.10,
    type: 6,
  },
  tcp: {
    src_port: 62589,
    dst_port: 4443,
  },
  community_id: "1:tSl1HyzM7qS0o3OpbOgxQJYCKCc=",
  udp: null,
  icmp: null,
}
```

If the trace contains 802.1Q traffic, then the output includes a `vlan` record:

```tql
{
  ether: {
    src: "00-17-5A-ED-7A-F0",
    dst: "FF-FF-FF-FF-FF-FF",
    type: 2048,
  },
  vlan: {
    outer: 1,
    inner: 20,
  },
  ip: {
    src: 192.168.1.1,
    dst: 255.255.255.255,
    type: 1,
  },
  icmp: {
    type: 8,
    code: 0,
  },
  community_id: "1:1eiKaTUjqP9UT1/1yu/o0frHlCk=",
}
```
