---
title: "Compress in-memory slices with Zstd"
type: feature
author: dominiklohmann
created: 2022-05-19T19:55:40Z
pr: 2268
---

VAST now compresses data with Zstd. When persisting data to the segment store,
the default configuration achieves over 2x space savings. When transferring data
between client and server processes, compression reduces the amount of
transferred data by up to 5x. This allowed us to increase the default partition
size from 1,048,576 to 4,194,304 events, and the default number of events in a
single batch from 1,024 to 65,536. The performance increase comes at the cost of
a ~20% memory footprint increase at peak load. Use the option
`vast.max-partition-size` to tune this space-time tradeoff.
