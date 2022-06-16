---
draft: true
title: VAST v2.1
description: VAST v2.1 - TODO
authors: dominiklohmann
date: 2022-06-16
tags: [release, performance]
---

[VAST v2.1][github-vast-release] is out! This release comes with a particular
focus on performance and reducing the size of VAST databases. It brings a new
utility for optimizing databases in production, allowing existing deployments to
take full advantage of the improvements after updating.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.1.0

<!--truncate-->

## Performance Improvements

VAST now compresses data with Zstd. When persisting data to the segment store,
the default configuration achieves over 2x space savings. When transferring data
between client and server processes, compression reduces the amount of
transferred data by up to 5x.

Additionally, VAST now compresses on-disk indexes with Zstd, resulting in a
50-80% size reduction depending on the type of indexes used.

This allowed us to increase the default partition size from 1,048,576 to
4,194,304 events[^1], and the default number of events in a single batch from 1,024
to 65,536, resulting in a massive performance increase at the cost of a ~20%
larger memory footprint at peak load. Use the option `vast.max-partition-size`
to tune this space-time tradeoff.

To benchmark this, we used [`speeve`][speeve] to generate 20 Eve JSON files
containing 8,388,608 events each[^2]. We spawned a VAST server process and ran
20 VAST client processes importing one file each in parallel. The numbers speak
for themselves:

||VAST v2.0|VAST v2.1|Change|
|-:|:-|:-|:-|
|Ingest Duration|1,650s|242s|-85.3%|
|Ingest Rate|101,680/s|693,273/s|+581.8%|
|Index Size|14,791MiB|5,721MiB|-61.3%|
|Store Size|37,656MiB|8,491MiB|-77.5%|
|Database Size|52,446MiB|14,212MiB|-72.9%|

:::note Compressed Filesystems
The above benchmarks ran on filesystems without compression. We expect the gain
from compression to be smaller when using compressed filesystems like
[`btrfs`][btrfs].
:::

[speeve]: https://github.com/satta/speeve
[btrfs]: https://btrfs.wiki.kernel.org/index.php/Main_Page

[^1]: VAST v2.0 failed to write its partitions to disk with the defaults for
  v2.1 because the on-disk size exceeded the maximum possible size of a
  FlatBuffers table, which VAST internally uses to have an open standard for its
  persistent state.
[^2]: This resulted in 167,772,160 events, with a total of 200'917'930 unique
  values with a schema distribution of 80.74% `suricata.flow`, 7.85%
  `suricata.dns`, 5.35% `suricata.http`, 4.57% `suricata.fileinfo`, 1.04%
  `suricata.tls`, 0.41% `suricata.ftp`, and 0.04% `suricata.smtp`.

## Optimizing VAST Databases

TODO: This is just copy-pasted from the changelog entry.

The new rebuild command allows for rebuilding old partitions to take advantage
of improvements by newer VAST versions. The rebuilding takes place in the VAST
server in the background. This process merges partitions up to the configured
`vast.max-partition-size,` turns VAST v1.x's heterogeneous into VAST v2.x's
homogenous partitions, migrates all data to the currently configured
store-backend, and upgrades to the most recent internal batch encoding and
indexes.
