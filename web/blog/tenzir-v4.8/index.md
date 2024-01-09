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
performance boost:

![Fluent Bit Performance](fluent-bit-performance.svg)

We used the following pipeline to generate these numbers:

```bash
tenzir --dump-metrics 'fluent-bit stdin | head 10M | discard' < eve.json
```

The `eve.json` input is from our Suricata dataset that we use in the [user
guides](/user-guides). We measured this on a 2021 Apple MacBook Pro M1 Max, as
well as on a Manjaro Linux laptop with a 14-core Intel i7 CPU. We don't have a
good explanation for the rather stark difference between the operating systems.
Our hunch is that the allocator performance is the high-order bit explaining the
difference.

Comparing the difference in absolute numbers might be tricky, but the gains
become obvious when comparing the relative speedup:

![Fluent Bit Speedup](fluent-bit-speedup.svg)

In summary, we observe a 3–5x gain for throughput in EPS and 4–8x improvement of
latency in terms of processing time.

Our primary goal was actually working around an issue with the Fluent Bit `lib`
output plugin, which we use whenever we have a Fluent Bit source operator. The `lib` plugin then takes the produced data and emits it into the Tenzir pipeline (`head` in the above example). During testing with the Fluent Bit `elasticsearch` and large Windows event logs, we noticed that Fluent Bit's `lib` output produces
messages of the form `[timestamp, JSON]` with cropped JSON.

The fix involved switching the exchange format of the `lib` output plugin from
JSON to MsgPack. If you're curious, take a look at
[#3770](https://github.com/tenzir/tenzir/pull/3770) for the full scoop.

Thanks to Christoph Lobmeyer and Yannik Meinhardt for tracking this issue! 🙏

## Here & There

If you're curious, [our changelog](/changelog#v480) has the full list of
changes.

Visit [app.tenzir.com](https://app.tenzir.com) to try the new
features and swing by [our Discord server](/discord) to get help and talk about
your use cases.
