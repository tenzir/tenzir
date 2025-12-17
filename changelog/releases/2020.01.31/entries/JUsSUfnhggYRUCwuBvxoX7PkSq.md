---
title: "Add separate interface option for import pcap"
type: change
author: dominiklohmann
created: 2019-11-07T19:04:10Z
pr: 641
---

The `import pcap` command no longer takes interface names via `--read,-r`, but
instead from a separate option named `--interface,-i`. This change has been made
for consistency with other tools.
