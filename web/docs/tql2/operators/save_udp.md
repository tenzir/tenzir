# save_udp

Saves bytes to a UDP socket.

```tql
save_udp endpoint:str, [insert_newlines=bool]
```

## Description

Saves bytes to a UDP socket.

### `endpoint: str`

The address of the remote endpoint to load bytes from. Must be of the format:
`[udp://]host:port`.

### `insert_newlines = bool (optional)`

Append a newline character (`\n`) at the end of every datagram.

This option comes in handy in combination with line-based parsers downstream,
such as NDJSON.

Defaults to `false`.

## Examples

Send the Tenzir version as CSV file to a remote endpoint via UDP:

```tql
version
write_csv
save_udp "127.0.0.1:56789"
```

Use `nc -ul 127.0.0.1 56789` to spin up a UDP server to test the above pipeline.
