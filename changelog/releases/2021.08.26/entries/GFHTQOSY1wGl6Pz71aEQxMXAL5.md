---
title: "Keep layer-2 framing when reading PCAP payload"
type: change
author: mavam
created: 2021-07-30T10:39:15Z
pr: 1797
---

VAST no longer strips link-layer framing when ingesting PCAPs. The stored
payload is the raw PCAP packet. Similarly, `vast export pcap` now includes a
Ethernet link-layer framing, per libpcap's `DLT_EN10MB` link type.
