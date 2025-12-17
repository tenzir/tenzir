---
title: "Prefix plugin library output names with vast-plugin-"
type: change
author: dominiklohmann
created: 2021-04-28T08:21:59Z
pr: 1593
---

To avoid confusion between the PCAP plugin and libpcap, which both have a
library file named `libpcap.so`, we now generally prefix the plugin library
output names with `vast-plugin-`. E.g., The PCAP plugin library file is now
named `libvast-plugin-pcap.so`. Plugins specified with a full path in the
configuration under `vast.plugins` must be adapted accordingly.
