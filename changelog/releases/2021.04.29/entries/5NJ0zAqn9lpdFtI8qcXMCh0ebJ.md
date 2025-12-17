---
title: "Move PCAP import/export into a plugin"
type: feature
author: dominiklohmann
created: 2021-04-16T11:36:07Z
pr: 1549
---

*Reader Plugins* and *Writer Plugins* are a new family of plugins that add
import/export formats. The previously optional PCAP format moved into a
dedicated plugin. Configure with `--with-pcap-plugin` and add `pcap` to
`vast.plugins` to enable the PCAP plugin.
