---
title: "Revamp packet acquisition and parsing"
type: feature
author: mavam
created: 2023-07-10T16:52:19Z
pr: 3263
---

The new `nic` plugin provides a loader that acquires packets from a network
interface card using libpcap. It emits chunks of data in the PCAP file format so
that the `pcap` parser can process them as if packets come from a trace file.

The new `decapsulate` operator processes events of type `pcap.packet` and emits
new events of type `tenzir.packet` that contain the decapsulated PCAP packet
with packet header fields from the link, network, and transport layer. The
operator also computes a Community ID.
