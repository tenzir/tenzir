---
title: Switching Fluent Bit from JSON to MsgPack
authors: [mavam]
date: 2024-01-10
tags: [fluent-bit, json, msgpack, performance]
comments: true
---

We re-wired Tenzir's [`fluent-bit`](/operators/fluent-bit) operator and
introduced a significant performance boost as a side effect: A 3–5x gain for
throughput in events per second (EPS) and 4–8x improvement of latency in terms
of processing time.

![Fluent Bit Speedup](fluent-bit-speedup.svg)

<!-- truncate -->

Why were these gains available? Because we eliminated one round-trip of internal
JSON printing and parsing.

## The Issue

Our primary goal was actually working around an issue
with the Fluent Bit `lib` output plugin, which we use whenever we have a
`fluent-bit` source operator. For example, if you configure an `elasticsearch`
Fluent Bit source, Tenzir's `fluent-bit` operator autocomplete the `lib` output
plugin. This plugin has two modes of accepting input: JSON or
[MsgPack](https://msgpack.org/).

Up to now, we relied on the JSON transfer mode because it was faster to get
started. However, during testing with the `elasticsearch` input that receives
large Windows event logs via Winlogbeat, we noticed that Fluent Bit's `lib`
output produces messages of the form `[timestamp, object]` where `object` was
cropped. This basically generated invalid JSON.

The fix involved switching the exchange format of the `lib` output plugin from
JSON to MsgPack. If you're curious, take a look at
[#3770](https://github.com/tenzir/tenzir/pull/3770) for the full scoop. The
improvement is already in the current development version and will be available
with the next release.

## Evaluation

We were curious how much this removal of the extra layer of printing and parsing
actually buys us. To this end, we use the following pipeline:

```bash
tenzir --dump-metrics 'fluent-bit stdin | head 10M | discard' < eve.json
```

Adding `--dump-metrics` adds detailed per-operator metrics that help us
understand where operators spend their time. The [`head`](/operators/head)
operator take the first 10 million events, and [`discard`](/operators/discard)
simply drops its input. The `eve.json` input into the `tenzir` binary is from
our Suricata dataset that we use in the [user guides](/usage). We measured
ran our measurements on a 2021 Apple MacBook Pro M1 Max, as well as on a Manjaro
Linux laptop with a 14-core Intel i7 CPU.

Our intuition was that we won't see major improvements, because generating JSON
isn't that expensive and we use [simdjson](https://simdjson.org/) to parse JSON.
But the results surprised us:

![Fluent Bit Performance](fluent-bit-performance.svg)

On macOS, events per second tripled from 50k to 150k, and the pipeline runtime
went from 42 to 10 seconds. On Linux, the improvements were even higher. We
don't have a good explanation for the rather stark difference between the
operating systems. Our hunch is that the allocator performance is the high-order
bit explaining the difference.

## Summary

We switched from JSON to MsgPack for our [`fluent-bit`](/operators/fluent-bit)
source operator. This removed one round-trip of printing JSON (in Fluent Bit)
and parsing JSON (in Tenzir). We were surprised to see that this change resulted
in such substantial performance improvements. As a result, you can now run many
more Fluent Bit ingestion pipelines in parallel at a single node with the same
resources, or vertically scale your Fluent Bit pipeline to new limits.

:::tip Acknowledgements
Thanks to Christoph Lobmeyer and Yannik Meinhardt for reporting this issue! 🙏
:::
