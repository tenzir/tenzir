---
title: "PRs 1819-1833"
type: feature
authors: satta
pr: 1819
---

VAST can now process Eve JSON events of type `suricata.packet` that Suricata
emits when the config option `tagged-packets` is set and a rule tags a packet
using, e.g., `tag:session,5,packets;`.
