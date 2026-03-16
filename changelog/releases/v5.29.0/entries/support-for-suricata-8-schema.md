---
title: Support for Suricata 8 schema
type: feature
authors:
  - IyeOnline
  - satta
pr: 5888
created: 2026-03-10T17:45:22.48402Z
---

The bundled Suricata schema now aligns with Suricata 8, enabling proper parsing and representation of events from Suricata 8 deployments.

This update introduces support for new event types including POP3, ARP, and BitTorrent DHT, along with enhancements to existing event types. QUIC events now include `ja4` and `ja4s` fields for fingerprinting, DHCP events include `vendor_class_identifier`, and TLS certificate timestamps now use the precise `time` type instead of string representation.

These schema changes ensure that Tenzir can reliably ingest and process telemetry from Suricata 8 without data loss or type mismatches.
