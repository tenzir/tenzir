---
title: "Add new debugging features to VAST tools"
type: feature
author: lava
created: 2022-05-31T12:44:50Z
pr: 2260
---

The `lsvast` tool can now print contents of individual `.mdx` files.
It now has an option to print raw Bloom filter contents of string
and IP address synopses.

The `mdx-regenerate` tool was renamed to `vast-regenerate` and can
now also regenerate an index file from a list of partition UUIDs.
