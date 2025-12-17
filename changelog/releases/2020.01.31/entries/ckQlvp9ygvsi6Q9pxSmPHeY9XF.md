---
title: "Allow configuring pcap snapshot length"
type: feature
author: dominiklohmann
created: 2019-11-08T14:06:02Z
pr: 642
---

The `import pcap` command now takes an optional snapshot length via `--snaplen`.
If the snapshot length is set to snaplen, and snaplen is less than the size of a
packet that is captured, only the first snaplen bytes of that packet will be
captured and provided as packet data.
