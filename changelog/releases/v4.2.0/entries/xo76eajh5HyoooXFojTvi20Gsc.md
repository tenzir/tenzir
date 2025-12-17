---
title: "Support parsing of concatenated PCAPs"
type: change
author: mavam
created: 2023-09-15T19:03:43Z
pr: 3513
---

The long option name `--emit-file-header` of the `pcap` parser is now called
`--emit-file-headers` (plural) to streamline it with the `nic` loader and the
new capability to process concatenated PCAP files.
