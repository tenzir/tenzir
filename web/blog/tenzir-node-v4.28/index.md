---
title: "Tenzir Node v4.28: Nested Parsing"
slug: tenzir-node-v4.28
authors: [dominiklohmann]
date: 2025-02-10
tags: [release, node]
comments: true
---

Tenzir Node v4.28 makes it easier than before to parse deeply nested data
structures.

![Tenzir Node v4.28](tenzir-node-v4.28.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.28.0

<!-- truncate -->

## Parsing Nested Data Structures

With this release, we've added a bunch of new functions for parsing nested data
from a string:

- `parse_kv` parses key-value pairs.
- `parse_grok` parses Grok patterns.
- `parse_csv`, `parse_ssv`, and `parse_tsv` parse comma-, space-, and
  tab-separated values, respectively.
- `parse_leef` and `parse_cef` parse LEEF and CEF data, respectively.
- `parse_syslog` parses Syslog data.
- `parse_json` parses JSON values.
- `parse_yaml` parses YAML values.

:::tip Read the Guide
To learn more about parsing nested data structures, check out our new [guide on
parsing nested data](/next/usage/parse-nested-data).
:::

The new `parse_*` functions behave similar to the `read_*` operators, except
that they work on one string field at a time instead of a stream of bytes in the
pipeline.

## VSCode TQL Package

If you're writing TQL in Visual Studio Code, you can now enjoy syntax
highlighting just like in the Tenzir Platform: The VSCode TQL package is now
available on the [Visual Studio Marketplace][vscode-tql].

[vscode-tql]: https://marketplace.visualstudio.com/items?itemName=tenzir.vscode-tql

## Let's Connect!

Want to be part of something exciting? Our vibrant community is waiting for you!
Drop into our bi-weekly office hours (every second Tuesday at 5 PM CET) on
[Discord][discord] where ideas flow freely, sneak peeks of new features abound,
and conversations spark between Tenzir enthusiasts and our passionate team.
Whether you've got burning questions, fascinating use cases to share, or just
want to hang out—our virtual door is always open!

[discord]: /discord
[changelog]: /changelog#v4280
