---
title: "Revamp packet acquisition and parsing"
type: change
authors: mavam
pr: 3263
---

We reimplemented the old `pcap` plugin as a format. The command `tenzir-ctl
import pcap` no longer works. Instead, the new `pcap` plugin provides a parser
that emits `pcap.packet` events, as well as a printer that generates a PCAP file
when provided with these events.
