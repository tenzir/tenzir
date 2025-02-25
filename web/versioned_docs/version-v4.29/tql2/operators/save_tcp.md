# save_tcp

Saves bytes to a TCP or TLS connection.

```tql
save_tcp endpoint:string, [tls=bool, skip_peer_verification=bool]
```

## Description

Saves bytes to the given endpoint via TCP or TLS.

### `endpoint: string`

The endpoint to which the server will connect. Must be of the form
`[tcp://]<hostname>:<port>`.

### `tls = bool (optional)`

Whether to use TLS.

Defaults to `false`.

### `skip_peer_verification = bool (optional)`

Whether to verify the TLS Certificate when connecting.

Defaults to `true` if TLS is being used.

:::tip Self-Signed Certificates
If you experience failures when using TLS with a Self-Signed Certificate,
try setting `skip_peer_verification=true`.
:::

## Examples

### Transform incoming Syslog to BITZ and save over TCP

```tql
load_tcp "0.0.0.0:8090" { read_syslog }
write_bitz
save_tcp "127.0.0.1:4000"
```

### Save to localhost with TLS

```tql
subscribe "feed"
write_json
save_tcp "127.0.0.1:4000", tls=true, skip_peer_verification=true
```
