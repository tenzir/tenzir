---
title: save_udp
category: Outputs/Bytes
example: 'save_udp "0.0.0.0:8090"'
---

Saves bytes to a UDP socket.

```tql
save_udp endpoint:str
```

## Description

Saves bytes to a UDP socket.

### `endpoint: str`

The address of the remote endpoint to load bytes from. Must be of the format:
`[udp://]host:port`.

## Examples

Send the Tenzir version as CSV file to a remote endpoint via UDP:

```tql
version
write_csv
save_udp "127.0.0.1:56789"
```

Use `nc -ul 127.0.0.1 56789` to spin up a UDP server to test the above pipeline.

## See Also

[`load_udp`](/reference/operators/load_udp)
