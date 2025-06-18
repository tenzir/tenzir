---
title: "Add separate interface option for import pcap"
type: change
authors: dominiklohmann
pr: 641
---

The `import pcap` command no longer takes interface names via `--read,-r`, but
instead from a separate option named `--interface,-i`. This change has been made
for consistency with other tools.
