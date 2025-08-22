---
title: from_udp
category: Inputs/Events
example: 'from_udp "0.0.0.0:8090"'
---

Receives UDP datagrams and outputs structured events.

```tql
from_udp endpoint:string, [resolve_hostnames=bool], [binary=bool]
```

## Description

Listens for UDP datagrams on the specified endpoint and outputs each datagram as
a structured event containing the data and peer information.

Unlike [`load_udp`](/reference/operators/load_udp), which outputs raw bytes,
`from_udp` produces structured events with metadata about the sender.

### `endpoint: string`

The address to listen on. Must be of the format: `[udp://]host:port`.

Use `0.0.0.0` as the host to accept datagrams on all interfaces. The
[`nics`](/reference/operators/nics) operator lists all available interfaces.

### `resolve_hostnames = bool (optional)`

Perform DNS lookups to resolve hostnames for sender IP addresses.

Defaults to `false` since DNS lookups can be slow and may impact performance
when receiving many datagrams.

### `binary = bool (optional)`

Output datagram data as binary (`blob`) instead of text (`string`).

Defaults to `false`. When `false`, the data field contains a UTF-8 string.
When `true`, the data field contains raw bytes as a blob.

## Output Schema

Each UDP datagram produces one event with the following structure:

```json
{
  "data": <string|blob>, // string by default, blob when binary=true
  "peer": {
    "ip": <ip>,
    "port": <uint64>,
    "hostname": <string> // Does not exist when `resolve_hostnames=false`
  }
}
```

## Examples

### Receive UDP datagrams with sender information

```tql
from_udp "0.0.0.0:1234"
```

This might output events like:

```json
{
  "data": "Hello World",
  "peer": {
    "ip": "192.168.1.10",
    "port": 5678
  }
}
```

### Parse JSON data from UDP datagrams

```tql
from_udp "127.0.0.1:8080"
select data = data.parse_json()
```

### Filter by sender and decode data

```tql
from_udp "0.0.0.0:9999"
where peer.ip == 192.168.1.100
select data
```

## See Also

[`load_udp`](/reference/operators/load_udp), [`save_udp`](/reference/operators/save_udp)
