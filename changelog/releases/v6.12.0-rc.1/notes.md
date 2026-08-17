Tenzir now receives Syslog reliably over RELP, including TLS and mutual TLS, with bounded acknowledgements and backpressure. This release also adds PCAPNG capture support and strengthens long-running Google SecOps and AWS pipelines.

## 🚀 Features

### PCAPNG capture support

The `read_pcap` and `write_pcap` operators now support PCAPNG captures. `read_pcap` detects PCAPNG input automatically, including captures with multiple interfaces, timestamp resolutions, byte orders, and sections. `write_pcap` preserves PCAPNG input automatically or generates PCAPNG explicitly:

```tql
write_pcap format="pcapng"
```

*By @mavam.*

### Receive Syslog over RELP

The new `accept_relp` operator receives Syslog over the Reliable Event Logging Protocol (RELP), including plaintext, TLS, and mutual TLS connections. It preserves complete RELP message boundaries and acknowledges each event after accepting it for processing:

```tql
accept_relp "0.0.0.0:2514"
syslog = data.parse_syslog()
```

An acknowledgement confirms that Tenzir accepted the event into a bounded in-memory handoff, not that a downstream destination stored it. RELP provides at-least-once transport, so the client may resend an event if the connection fails before it receives the acknowledgement.

*By @mavam.*

## 🐞 Bug fixes

### Bounded buffering and coordinated retries for Google SecOps

The `to_google_secops` operator now bounds buffered request batches according to `parallel` and propagates backpressure upstream when all request slots are occupied.

Requests that receive a `429` or `5XX` response now share one retry cooldown. After the cooldown, the operator sends one probe request at a time, honors `Retry-After`, and resumes queued requests when a request succeeds. Retryable responses no longer cause request batches to be discarded after a fixed number of attempts.

*By @raxyte.*

### Fixed event delivery for from_fluent_bit in neo mode

Fixed an issue that caused `from_fluent_bit` to not deliver any events in neo mode.

*By @lava.*

### Refresh assumed-role AWS credentials

AWS-backed operators now refresh assumed-role sessions when `aws_iam` combines an explicit access key and secret key with `assume_role`. Previously, these pipelines failed when the initial STS session expired, typically after one hour.

*By @raxyte.*
