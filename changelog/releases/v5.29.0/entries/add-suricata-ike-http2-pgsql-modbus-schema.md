---
title: Add Suricata schema types for IKE, HTTP2, PGSQL, and Modbus
type: feature
authors:
  - tobim
pr: 5914
created: 2026-03-17T00:00:00Z
---

The bundled Suricata schema now includes types for four previously missing event types: `ike`, `http2`, `pgsql`, and `modbus`.

The `ike` type supports both IKEv1 and IKEv2 traffic. Version-specific fields are contained within dedicated `ikev1` and `ikev2` sub-objects, covering key exchange payloads, nonce payloads, client proposals, vendor IDs, and IKEv2 role/notify information.

The `http2` type models HTTP/2 request and response streams including settings frames, header lists, error codes, and stream priority.

The `pgsql` type covers PostgreSQL session events with full request fields (simple queries, startup parameters, SASL authentication) and response fields (row counts, command completion, parameter status).

The `modbus` type captures industrial Modbus protocol transactions including function codes, access types, exception responses, diagnostic subfunctions, and MEI encapsulated interface data.
