# read_pcap

Reads raw network packets in [PCAP][pcap-rfc] file format.

[pcap-rfc]: https://datatracker.ietf.org/doc/id/draft-gharris-opsawg-pcap-00.html

```tql
read_pcap [emit_file_headers=bool]
```

## Description

The `read_pcap` operator converts raw bytes representing a [PCAP][pcap-rfc] file into
events.

[pcapng-rfc]: https://www.ietf.org/archive/id/draft-tuexen-opsawg-pcapng-05.html

:::note PCAPNG
The current implementation does *not* support [PCAPNG][pcapng-rfc]. Please
[reach out](/discord) if you would like to see support.
:::

### `emit_file_headers = bool (optional)`

Emit a `pcap.file_header` event that represents the PCAP file header. If
present, the parser injects this additional event before the subsequent stream
of packets.

Emitting this extra event makes it possible to seed the [`write_pcap`](write_pcap.md) operator with a
file header from the input. This allows for controlling the timestamp formatting
(microseconds vs. nanosecond granularity) and byte order in the packet headers.

When the PCAP parser processes a concatenated stream of PCAP files, specifying
`emit_file_headers` will also re-emit every intermediate file header as
separate event.

Use this option when you would like to reproduce the identical trace file layout
of the PCAP input.

## Schemas

The operator emits events with the following schema.

### `pcap.packet`

Contains information about all accessed API endpoints, emitted once per second.

| Field                    | Type       | Description                                            |
| :----------------------- | :--------- | :----------------------------------------------------- |
| `timestamp`              | `time`     | The time of capturing the packet.                      |
| `linktype`               | `uint64`   | The linktype of the captured packet.                   |
| `original_packet_length` | `uint64`   | The length of the original packet.                     |
| `captured_packet_length` | `uint64`   | The length of the captured packet.                     |
| `data`                   | `blob`     | The captured packet's data as a blob.                  |

## Examples

### Read packets from a PCAP file:

```tql
load_file "/tmp/trace.pcap"
read_pcap
```

### Read packets from the [network interface](load_nic.md) `eth0`:

```tql
load_nic "eth0"
read_pcap
```
