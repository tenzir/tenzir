Tenzir nodes now honor standard HTTP proxy environment variables when connecting to the Tenzir Platform, and hash functions produce correct checksums for binary values.

## 🚀 Features

### Platform websocket proxy support

Tenzir nodes now honor standard HTTP proxy environment variables when connecting to Tenzir Platform:

```sh
HTTPS_PROXY=http://proxy.example:3128 tenzir-node
```

Use `NO_PROXY` to bypass the proxy for selected hosts. This helps deployments where outbound connections to the Platform websocket gateway must go through an HTTP proxy.

*By @tobim and @codex in #6039.*

## 🔧 Changes

### Add `accept_http` operator for receiving HTTP requests

We added a new operator to accept data from incoming HTTP connections.

The `server` option of the `from_http` operator is now deprecated. Going forward, it should only be used for client-mode HTTP operations, and the new `accept_http` operator should be used for server-mode operations.

*By @lava.*

## 🐞 Bug fixes

### Raw-byte hashing for binary values

The `hash_*` functions now hash `blob` values by their raw bytes. This makes checksums computed from binary data match external tools such as `md5sum` and `sha256sum`.

For example:

```tql
from_file "trace.pcap" {
  read_all binary=true
}
md5 = data.hash_md5()
```

This is useful for verifying file contents and round-tripping binary formats without leaving TQL.

*By @mavam and @codex in #6022.*
