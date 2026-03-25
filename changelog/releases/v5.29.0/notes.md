This release improves log ingestion by extracting structured data from legacy syslog messages and aligning the bundled schema with Suricata 8. It also republishes the previous release after an error in the earlier release process.

## 🚀 Features

### Extract structured data from legacy syslog content

`read_syslog` and `parse_syslog` now extract a leading RFC 5424-style structured-data block from RFC 3164 message content.

This pattern occurs in practice with some VMware ESXi messages, where components such as `Hostd` emit a legacy syslog record and prepend structured metadata before the human-readable message text.

For example, this raw syslog line:

```text
<166>2026-02-11T18:01:45.587Z esxi-01.example.invalid Hostd[2099494]: [Originator@6876 sub=Vimsvc.TaskManager opID=11111111-2222-3333-4444-555555555555] Task Completed
```

now parses as:

```tql
{
  facility: 20,
  severity: 6,
  timestamp: "2026-02-11T18:01:45.587Z",
  hostname: "esxi-01.example.invalid",
  app_name: "Hostd",
  process_id: "2099494",
  structured_data: {
    "Originator@6876": {
      sub: "Vimsvc.TaskManager",
      opID: "11111111-2222-3333-4444-555555555555",
    },
  },
  content: "Task Completed",
}
```

Events without extracted structured data keep the existing `syslog.rfc3164` schema. Events with extracted structured data use `syslog.rfc3164.structured`.

*By @mavam and @codex in #5902.*

### Support for Suricata 8 schema

The bundled Suricata schema now aligns with Suricata 8, enabling proper parsing and representation of events from Suricata 8 deployments.

This update introduces support for new event types including POP3, ARP, and BitTorrent DHT, along with enhancements to existing event types. QUIC events now include `ja4` and `ja4s` fields for fingerprinting, DHCP events include `vendor_class_identifier`, and TLS certificate timestamps now use the precise `time` type instead of string representation.

These schema changes ensure that Tenzir can reliably ingest and process telemetry from Suricata 8 without data loss or type mismatches.

*By @IyeOnline and @satta in #5888.*

## 🐞 Bug fixes

### Fix pipeline startup timeouts

In some situations, pipelines could not be successfully started, leading to timeouts and a non-responsive node, especially during node start.

*By @jachris in #5893.*

### Graceful handling of Google Cloud Pub/Sub authentication errors

Invalid Google Cloud credentials in `from_google_cloud_pubsub` no longer crash the node. Authentication errors now surface as operator diagnostics instead.

*By @mavam and @codex in #5877.*

### Prevent where/map assertion crash on sliced list batches

Pipelines using chained list transforms such as `xs.where(...).map(...).where(...)` no longer trigger an internal assertion on sliced input batches.

*By @IyeOnline and @codex in #5886.*
