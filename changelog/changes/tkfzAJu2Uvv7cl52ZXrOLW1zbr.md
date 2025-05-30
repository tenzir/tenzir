---
title: "Ignore VLAN tags in PCAP import"
type: bugfix
authors: mavam
pr: 650
---

PCAP ingestion failed for traces containing VLAN tags. VAST now strips [IEEE
802.1Q](https://en.wikipedia.org/wiki/IEEE_802.1Q) headers instead of skipping
VLAN-tagged packets.
