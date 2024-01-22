---
title: Tenzir v4.9
authors: [dominiklohmann]
date: 2024-03-01
tags: [lookup, bloom-filter]
draft: true
comments: true
---

[Tenzir
v4.9](https://github.com/tenzir/tenzir/releases/tag/v4.9.0) is out.

<!--![Tenzir v4.9](tenzir-v4.9.excalidraw.svg)-->

<!-- truncate -->

## Bloom Filter Context

The new [`bloom-filter`](/next/contexts/bloom-filter) context makes it possible
to use large datasets for enrichment. It uses a [Bloom
filter](https://en.wikipedia.org/wiki/Bloom_filter) to store sets in a compact
way, at the cost of potential false positives when looking up an item.

If you have massive amounts of indicators or a large amount of things you would
liket to contextualize, this feature is for you.

## Housekeeping

For the curious, [the changelog](/changelog#v490) has the full scoop.

Visit [app.tenzir.com](https://app.tenzir.com) to try the new
features and swing by [our Discord server](/discord) to get help and talk about
your use cases.
