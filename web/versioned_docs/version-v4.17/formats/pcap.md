---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# pcap

Reads and writes raw network packets in [PCAP][pcap-rfc] file format.

[pcap-rfc]: https://datatracker.ietf.org/doc/id/draft-gharris-opsawg-pcap-00.html

## Synopsis

Parser:

```
pcap [-e|--emit-file-headers]
```

Printer:

```
pcap
```

## Description

The `pcap` parser converts raw bytes representing a [PCAP][pcap-rfc] file into
events, and the `pcap` printer generates a PCAP file from events.

[pcapng-rfc]: https://www.ietf.org/archive/id/draft-tuexen-opsawg-pcapng-05.html

:::note PCAPNG
The current implementation does *not* support [PCAPNG][pcapng-rfc]. Please
[reach out](/discord) if you would like to see support.
:::

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

### `-e|--emit-file-headers` (Parser)

Emit a `pcap.file_header` event that represents the PCAP file header. If
present, the parser injects this additional event before the subsequent stream
of packets.

Emitting this extra event makes it possible to seed the `pcap` printer with a
file header from the input. This allows for controlling the timestamp formatting
(microseconds vs. nanosecond granularity) and byte order in the packet headers.

When the PCAP parser processes a concatenated stream of PCAP files, specifying
`--emit-file-headers` will also re-emit every intermediate file header as
separate event.

Use this option when you would like to reproduce the identical trace file layout
of the PCAP input.

## Examples

Read packets from a PCAP file:

```
from file /tmp/trace.pcap read pcap
```

Read packets from the [network interface](../connectors/nic.md) `eth0`:

```
from nic eth0 read pcap
```

[Decapsulate](../operators/decapsulate.md) packets in a PCAP
file:

```
read pcap | decapsulate
```

On the command line, merge PCAP files and process parse them:

```bash
cat *.pcap | tenzir 'read pcap'
```
