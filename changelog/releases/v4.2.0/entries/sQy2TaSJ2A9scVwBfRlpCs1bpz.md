---
title: "Do not drop the `data` field in `decapsulate`"
type: change
author: dominiklohmann
created: 2023-09-19T11:13:13Z
pr: 3515
---

The `decapsulate` operator no longer drops the PCAP packet data in incoming
events.
