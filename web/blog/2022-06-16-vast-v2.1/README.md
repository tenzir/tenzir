---
draft: true
title: VAST v2.1
description: VAST v2.1 - TODO
authors: dominiklohmann
slug: 2022-06-16/vast-v2.1
tags: [release, performance]
---

[VAST v2.1][github-vast-release] is out! This release comes with a particular
focus on performance, and brings a new utility for optimizing databases in
production.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.1.0

<!--truncate-->

## Performance Improvements

TODO: This is just copy-pasted from the changelog entry.

VAST now compresses data with Zstd. When persisting data to the segment store,
the default configuration achieves over 2x space savings. When transferring data
between client and server processes, compression reduces the amount of
transferred data by up to 5x. This allowed us to increase the default partition
size from 1,048,576 to 4,194,304 events, and the default number of events in a
single batch from 1,024 to 65,536. The superior performance increase comes at
the cost of a ~20% memory footprint increase at peak load. Use the option
`vast.max-partition-size` to tune this space-time tradeoff.

## Optimizing VAST Databases

TODO: This is just copy-pasted from the changelog entry.

The new rebuild command allows for rebuilding old partitions to take advantage
of improvements by newer VAST versions. The rebuilding takes place in the VAST
server in the background. This process merges partitions up to the configured
`vast.max-partition-size,` turns VAST v1.x's heterogeneous into VAST v2.x's
homogenous partitions, migrates all data to the currently configured
store-backend, and upgrades to the most recent internal batch encoding and
indexes.
