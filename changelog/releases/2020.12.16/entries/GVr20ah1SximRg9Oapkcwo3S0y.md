---
title: "PRs 1176-1180-1186-1237-satta"
type: change
author: satta
created: 2020-11-17T19:13:29Z
pr: 1176
---

The Suricata schemas received an overhaul: there now exist `vlan` and `in_iface`
fields in all types. In addition, VAST ships with new types for `ikev2`, `nfs`,
`snmp`, `tftp`, `rdp`, `sip` and `dcerpc`. The `tls` type gets support for the
additional `sni` and `session_resumed` fields.
