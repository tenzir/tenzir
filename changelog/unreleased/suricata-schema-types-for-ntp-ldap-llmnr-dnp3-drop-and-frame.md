---
title: Suricata schema types for NTP, LDAP, LLMNR, DNP3, drop, and frame events
type: feature
authors:
  - satta
created: 2026-09-27T00:00:00Z
---

The bundled Suricata schema now includes types for more EVE JSON event types: `suricata.ntp`, `suricata.ldap`, `suricata.llmnr`, `suricata.dnp3`, `suricata.drop`, and `suricata.frame`. `read_suricata` now parses these events with proper types and no longer warns that the schema is unknown.

The `suricata.http` type now also covers HTTP/2 transactions, which Suricata logs as `http` events. These events carry the `version`, `request_headers`, and `response_headers` fields, and nest the HTTP/2-specific data under `http.http2`. DNS SOA records now include the `mname_truncated` and `rname_truncated` flags.
