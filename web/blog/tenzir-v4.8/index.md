---
title: Tenzir v4.8
authors: [dominiklohmann]
date: 2024-02-01
tags: [fluent-bit, performance]
draft: true
comments: true
---

[Tenzir v4.8](https://github.com/tenzir/tenzir/releases/tag/v4.8.0) .

<!--![Tenzir v4.8](tenzir-v4.8.excalidraw.svg)-->

<!-- truncate -->

## Theme A

TBD

## Theme B

TBD

## Theme C

TBD

## Fluent Bit Performance

The [`fluent-bit`](/operators/fluent-bit) source operator got a significant
performance boost as a byproduct of changing the Fluent Bit data exchange format
from JSON to MsgPack:

![Fluent Bit Performance](fluent-bit-speedup.svg)

Read the [dedicated blog post on this
issue](/blog/switching-fluentbit-from-json-to-msgpack).

Thanks to Christoph Lobmeyer and Yannik Meinhardt for reporting this issue! 🙏

## Here & There

If you're curious, [our changelog](/changelog#v480) has the full list of
changes.

Visit [app.tenzir.com](https://app.tenzir.com) to try the new
features and swing by [our Discord server](/discord) to get help and talk about
your use cases.
