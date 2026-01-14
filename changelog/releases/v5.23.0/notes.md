This release introduces centralized node-level TLS configuration, allowing you to configure TLS settings once in tenzir.yaml instead of passing options to each operator individually. It also adds support for event-timestamp-based compaction rules and a count field in the deduplicate operator.

## 🚀 Features

### Count dropped events in deduplicate operator

The `deduplicate` operator now supports a `count_field` option that adds a field to each output event showing how many events were dropped for that key.

**Example**

```tql
from {x: 1, seq: 1}, {x: 1, seq: 2}, {x: 1, seq: 3}, {x: 1, seq: 4}
deduplicate x, distance=2, count_field=drop_count
```

```tql
{x: 1, seq: 1, drop_count: 0}
{x: 1, seq: 4, drop_count: 2}
```

Events that are the first occurrence of a key or that trigger output after expiration have a count of `0`.

*By @raxyte in #5622.*

### Node-level TLS configuration for operators

All operators and connectors that use TLS now support centralized node-level configuration. Instead of passing TLS options to each operator individually, you can configure them once in `tenzir.yaml` under `tenzir.tls`.

Arguments passed directly to the operator itself via an argument take precedence over the configuration entry.

The following options are available:

- `enable`: Enable TLS on all operators that support it
- `skip-peer-verification`: Disable certificate verification
- `cacert`: Path to a CA certificate bundle for server verification
- `certfile`: Path to a client certificate file
- `keyfile`: Path to a client private key file
- `tls-min-version`: Minimum TLS protocol version (`"1.0"`, `"1.1"`, `"1.2"`, or `"1.3"`)
- `tls-ciphers`: OpenSSL cipher list string

The later two options have also been added as operator arguments.

For server-mode operators (`load_http server=true`, `load_tcp`), mutual TLS (mTLS) authentication is now supported:

- `tls-client-ca`: Path to a CA certificate for validating client certificates
- `tls-require-client-cert`: Require clients to present valid certificates

These two options are also available as operator arguments.

Example configuration enforcing TLS 1.2+ with specific ciphers:

```yaml
tenzir:
  tls:
    tls-min-version: "1.2"
    tls-ciphers: "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256"
    cacert: "/etc/ssl/certs/ca-certificates.crt"
```

*By @IyeOnline in #5559.*

### Platform TLS configuration

The Tenzir Node now lets you configure the minimum TLS version and TLS ciphers accepted for the connection to the Tenzir Platform:

```yaml
plugins:
  platform:
    tls-min-version: "1.2"
    tls-ciphers: "HIGH:!aNULL:!MD5"
```

*By @lava in #5559.*

### Use event timestamps for compaction rules

Compaction rules can now use event timestamps instead of import time when selecting data by age. Configure this using the new optional `field` key in the compaction configuration.

Previously, compaction always used the import time to determine which partitions to compact. Now you can specify any timestamp field from your events:

```yaml
tenzir:
  compaction:
    time:
      rules:
        - name: compact-old-logs
          after: 7d
          field: timestamp  # Use event timestamp instead of import time
          pipeline: |
            summarize count=count(), src_ip
```

When `field` is not specified, compaction continues to use import time for backward compatibility.

*By @jachris in #5629.*

## 🐞 Bug fixes

### Fixed default compaction rules for metrics and diagnostics

The default compaction rules for `tenzir.metrics.*` and `tenzir.diagnostic` events now correctly use the `timestamp` field instead of import time.

Previously, these built-in compaction rules relied on import time to determine which events to compact, which could lead to inconsistent results as the import time is not computed per-event. As a result, it was possible that metrics and diagnostics were not deleted even though they expired.

*By @jachris in #5629.*
