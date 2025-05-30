---
title: "Change type of `version` in `suricata.quic` to `string`"
type: bugfix
authors: jachris
pr: 3533
---

The type of the `quic.version` field in the built-in `suricata.quic` schema was
fixed. It now is a string instead of an integer.
