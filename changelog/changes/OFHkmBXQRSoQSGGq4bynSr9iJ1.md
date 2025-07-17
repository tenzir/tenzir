---
title: "Add missing concepts for Suricata events"
type: bugfix
authors: tobim
pr: 1798
---

Previously missing fields of suricata event types are now part of the concept
definitions of `net.src.ip`, `net.src.port`, `net.dst.ip`, `net.dst.port`,
`net.app`, `net.proto`, `net.community_id`, `net.vlan`, and `net.packets`.
