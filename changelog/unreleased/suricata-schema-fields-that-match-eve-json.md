---
title: Suricata schema fields that match EVE JSON
type: bugfix
authors:
  - satta
created: 2026-10-12T00:00:00Z
---

The bundled Suricata schema now matches the EVE JSON that Suricata writes in more places:

- `read_suricata` no longer warns about and drops `dns.type` for DNS events in the version 3 format of Suricata 8, whose values are `request` and `response`.
- The `has_multiple` field of HTTP/2 transactions is now a string, such as `priority`, instead of a boolean that never parsed.
- `rdp.client.product_id` is now an integer instead of a string.
- The schema now expects the `tcp` object of `flow` and `netflow` events at the top level, the IKEv2 `role` in `ike`, and Modbus `exception`, `diagnostic`, and `mei` data inside `modbus.request` and `modbus.response`, where Suricata writes them.

The schema no longer contains fields that Suricata never writes: `alert.app_proto`, `headers` inside `http.http2`, and the unused `suricata.http2` type.
