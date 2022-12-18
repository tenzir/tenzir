---
title: VAST v2.4.1
description: Faster Initial Results
authors: dominiklohmann
date: 2022-12-19
tags: [release, feather, performance]
---

[VAST v2.4.1][github-vast-release] improves the performance of queries when VAST
is under high load, and significantly reduces the time to first result for
queries with a low selectivity.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.4.1

<!--truncate-->

## Reading Feather Files Incrementally

VAST's Feather store naïvely used the [Feather reader][feather-reader] from the
Apache Arrow C++ library in its initial implementation. However, its API is
rather limited: It does not support reading incrementally. We've swapped this
out with a more efficient implementation that does.

[feather-reader]: https://github.com/apache/arrow/blob/apache-arrow-10.0.1/cpp/src/arrow/ipc/feather.h#L57-L108

This is best explained visually:

![Incremental Reads](incremental-reads-light.png#gh-light-mode-only)
![Incremental Reads](incremental-reads-dark.png#gh-dark-mode-only)

Within the scope of a single store, queries take the same amount of time
overall, but there exist two distinct advantages of this approach:
1. The first result arrives much faster at the client.
2. Stores do less work for cancelled queries.

One additional benefit that is not immediately obvious only comes into play a
query arrives at multiple stores in parallel. Because the dksk reads are more
evenly spread out now, the disk reads are less likely to overlap between stores
queries in parallel. For disk-read performance-bound deployments this can lead
to a significant query performance improvement.

To verify and test this, I've created a VAST database with 300M Zeek events from
a Corelight sensor. All tests were performed on a cold start of VAST, i.e., I
stopped and started VAST after every repetition of each test.

I performed three tests:
1. Export a single event (20 repetitions)
2. Export all events (20 repetitions)
3. Rebuild the entire database (3 repetitions)

The results are astonishingly good:

|Test|Benchmark|v2.4.0|v2.4.1|Improvement|
|:-:|:-:|:-:|:-:|:-:|
|**(1)**|Avg. store load time|55.1ms|4.2ms|13.1x|
||Time to first result/Total time|19.8ms|14.5ms|1.4x|
|**(2)**|Avg. store load time|386.5ms|7.3ms|52.9x|
||Time to first result|69.2ms|25.4ms|2.7x|
||Total time|39.38s|33.30s|1.2x|
|**(3)**|Avg. store load time|480.3ms|9.1ms|52.7x|
||Total time|210.5s|198.0s|1.1x|
