---
title: "PRs 827-844"
type: feature
author: dominiklohmann
created: 2020-04-15T13:52:18Z
pr: 827
---

Packet drop and discard statistics are now reported to the accountant for PCAP
import, and are available using the keys `pcap-reader.recv`, `pcap-reader.drop`,
`pcap-reader.ifdrop`, `pcap-reader.discard`, and `pcap-reader.discard-rate` in
the `vast.statistics` event. If the number of dropped packets exceeds a
configurable threshold, VAST additionally warns about packet drops on the
command line.
