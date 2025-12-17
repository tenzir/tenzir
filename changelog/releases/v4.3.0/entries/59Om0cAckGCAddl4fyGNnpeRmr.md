---
title: "Change type of `version` in `suricata.quic` to `string`"
type: bugfix
author: jachris
created: 2023-09-21T14:13:40Z
pr: 3533
---

The type of the `quic.version` field in the built-in `suricata.quic` schema was
fixed. It now is a string instead of an integer.
