---
title: "Move PCAP import/export into a plugin"
type: feature
authors: dominiklohmann
pr: 1549
---

*Reader Plugins* and *Writer Plugins* are a new family of plugins that add
import/export formats. The previously optional PCAP format moved into a
dedicated plugin. Configure with `--with-pcap-plugin` and add `pcap` to
`vast.plugins` to enable the PCAP plugin.
