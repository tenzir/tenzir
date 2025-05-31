---
title: write_pcap
category: Printing
example: 'write_pcap'
---
Transforms event stream to PCAP byte stream.

```tql
write_pcap
```

## Description

Transforms event stream to [PCAP][pcap-rfc] byte stream.

[pcap-rfc]: https://datatracker.ietf.org/doc/id/draft-gharris-opsawg-pcap-00.html

The structured representation of packets has the `pcap.packet` schema:

```yaml
pcap.packet:
  record:
    - linktype: uint64
    - time:
        timestamp: time
    - captured_packet_length: uint64
    - original_packet_length: uint64
    - data: string
```

:::note[PCAPNG]
The current implementation does *not* support [PCAPNG][pcapng-rfc].
:::

[pcapng-rfc]: https://www.ietf.org/archive/id/draft-tuexen-opsawg-pcapng-05.html

## Examples

### Write packet events as a PCAP file

```tql
subscribe "packets"
write_pcap
save_file "/logs/packets.pcap"
```

## See Also

[`load_nic`](/reference/operators/load_nic),
[`read_pcap`](/reference/operators/read_pcap)
