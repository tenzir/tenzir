---
title: "Support parsing of concatenated PCAPs"
type: change
authors: mavam
pr: 3513
---

The long option name `--emit-file-header` of the `pcap` parser is now called
`--emit-file-headers` (plural) to streamline it with the `nic` loader and the
new capability to process concatenated PCAP files.
