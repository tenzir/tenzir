---
title: "Support parsing of concatenated PCAPs"
type: feature
author: mavam
created: 2023-09-15T19:03:43Z
pr: 3513
---

The `pcap` parser can now process a stream of concatenated PCAP files. On the
command line, you can now parse traces with `cat *.pcap | tenzir 'read pcap'`.
When providing `--emit-file-headers`, each intermediate file header yields a
separate event.

The `nic` loader has a new option `--emit-file-headers` that prepends a PCAP
file header for every batch of bytes that the loader produces, yielding a
stream of concatenated PCAP files.
