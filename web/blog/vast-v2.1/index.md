---
title: VAST v2.1
description: VAST v2.1 - Tune VAST Databases
authors: dominiklohmann
date: 2022-07-07
tags: [release, performance]
---

[VAST v2.1][github-vast-release] is out! This release comes with a particular
focus on performance and reducing the size of VAST databases. It brings a new
utility for optimizing databases in production, allowing existing deployments to
take full advantage of the improvements after upgrading.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.1.0

<!--truncate-->

## New Project Site

VAST has new project site: [vast.io](https://vast.io). We ported all
documentation from `https://docs.tenzir.com`, added a lot of new content, and
restructured the reading experience along the user journey.

You can find the Threat Bus documentation in [Use VAST → Integrate → Threat
Bus](/VAST%20v3.0/use/integrate/threatbus). Threat Bus is now officially in
maintainance mode: we are only supporting existing features with bugfixes. That
said, Threat Bus will resurface in a new shape with its existing functionality
integrated into VAST itself. Stay tuned.

## Performance Improvements

VAST now compresses data with [Zstd](http://www.zstd.net). The default
configuration achieves over 2x space savings. When transferring data between
client and server processes, compression reduces the amount of transferred data
by up to 5x.

Additionally, VAST now compresses on-disk indexes with Zstd, resulting in a
50-80% size reduction depending on the type of indexes used.

This allowed us to increase the default partition size from 1,048,576 to
4,194,304 events[^1], and the default number of events in a single batch from 1,024
to 65,536, resulting in a massive performance increase at the cost of a ~20%
larger memory footprint at peak loads. Use the option `vast.max-partition-size`
to tune this space-time tradeoff.

To benchmark this, we used [`speeve`][speeve] to generate 20 EVE JSON files
containing 8,388,608 events each[^2]. We spawned a VAST server process and ran
20 VAST client processes in parallel, with one process per file.

We observed a reduction of **up to 73%** of disk space utilization:

![Database Size](storage-light.png#gh-light-mode-only)
![Database Size](storage-dark.png#gh-dark-mode-only)

In addition, we were able to scale the ingest rate by almost **6x** due to the
higher batch size and the reduced memory usage per batch:

![Ingest Rate](rate-light.png#gh-light-mode-only)
![Ingest Rate](rate-dark.png#gh-dark-mode-only)

The table below summaries the benchmarks:

||VAST v2.0|VAST v2.1|Change|
|-:|:-|:-|:-|
|Ingest Duration|1,650 s|242 s|-85.3%|
|Ingest Rate|101,680 events/s|693,273 events/s|+581.8%|
|Index Size|14,791 MiB|5,721 MiB|-61.3%|
|Store Size|37,656 MiB|8,491 MiB|-77.5%|
|Database Size|52,446 MiB|14,212 MiB|-72.9%|

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

## Rebuild VAST Databases

The new changes to VAST's internal data format only apply to newly ingested
data. To retrofit changes, we introduce a new `rebuild` command with this
release. A rebuild effectively re-ingests events from existing partitions and
atomically replaces them with partitions of the new format.

This makes it possible to upgrade persistent state to a newer version, or
recreate persistent state after changing configuration parameters, e.g.,
switching from the Feather to the Parquet store backend (that will land in
v2.2). Rebuilding partitions also recreates their sparse indexes that
accellerate query execution. The process takes place asynchronously in the
background.

We recommend running `vast rebuild` to upgrade your VAST v1.x partitions to VAST
v2.x partitions to take advantage of the new compression and an improved
internal representation.

This is how you run it:

```bash
vast rebuild [--all] [--undersized] [--parallel=<number>] [<expression>]
```

A rebuild is not only useful when upgrading outdated partitions, but also when
changing parameters of up-to-date partitions. Use the `--all` flag to extend a
rebuild operation to _all_ partitions. (Internally, VAST versions the partition
state via FlatBuffers. An outdated partition is one whose version number is not
the newest.)

The `--undersized` flag causes VAST to only rebuild partitions that are under
the configured partition size limit `vast.max-partition-size`.

The `--parallel` options is a performance tuning knob. The parallelism level
controls how many sets of partitions to rebuild in parallel. This value defaults
to 1 to limit the CPU and memory requirements of the rebuilding process, which
grow linearly with the selected parallelism level.

An optional expression allows for restricting the set of partitions to rebuild.
VAST performs a catalog lookup with the expression to identify the set of
candidate partitions. This process may yield false positives, as with regular
queries, which may cause unaffected partitions to undergo a rebuild. For
example, to rebuild outdated partitions containing `suricata.flow` events
older than 2 weeks, run the following command:

```bash
vast rebuild '#type == "suricata.flow" && #import_time < 2 weeks ago'
```
