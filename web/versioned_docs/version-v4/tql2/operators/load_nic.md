# load_nic

Loads bytes from a network interface card (NIC).

[pcap-rfc]: https://datatracker.ietf.org/doc/id/draft-gharris-opsawg-pcap-00.html

```tql
load_nic iface:str, [snaplen=int, emit_file_headers=bool]
```

## Description

The `load_nic` operator uses libpcap to acquire packets from a network interface and
packs them into blocks of bytes that represent PCAP packet records.

The received first packet triggers also emission of PCAP file header such that
downstream operators can treat the packet stream as valid PCAP capture file.

### `iface: str`

The interface to load bytes from.

### `snaplen = int (optional)`

Sets the snapshot length of the captured packets.

This value is an upper bound on the packet size. Packets larger than this size
get truncated to `snaplen` bytes.

Defaults to `262144`.

### `emit_file_headers = bool (optional)`

Creates PCAP file headers for every flushed batch.

The operator emits chunk of bytes that represent a stream of packets.
When setting `emit_file_headers` every chunk gets its own PCAP file header, as
opposed to just the very first. This yields a continuous stream of concatenated
PCAP files.

The [`pcap`](read_pcap.md) parser can handle such concatenated traces, and
optionally re-emit thes file headers as separate events.

## Examples

### Read PCAP packets from `eth0`

```tql
load_nic "eth0"
read_pcap
```

### Perform the equivalent of `tcpdump -i en0 -w trace.pcap`

```tql
load_nic "en0"
read_pcap
write_pcap
save_file "trace.pcap"
```
