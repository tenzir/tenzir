---
draft: true
title: VAST v2.3
description: Automatic Rebuilds
authors: dominiklohmann
date: 2022-08-19
tags: [release, rebuild, performance]
---

[VAST v2.3][github-vast-release] is now available. This release brings an
automatic defragmentation and updating process to VAST.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.3.0

<!--truncate-->

## Automatic Rebuilds

VAST server processes now continuously rebuild outdated and merge undersized
partitions in the background. The following diagram illustrates this
"defragmentation" procedure:

![Rebuild](/img/rebuild-light.png#gh-light-mode-only)
![Rebuild](/img/rebuild-dark.png#gh-dark-mode-only)

To control this behavior, set the new `vast.automatic-rebuild` option.

```yaml
vast:
  # Control automatic rebuilding of partitions in the background for
  # optimization purposes. The given number controls how many rebuilds to run
  # concurrently, and thus directly controls the performance vs. memory and CPU
  # usage trade-off. Set to 0 to disable. Defaults to 1.
  automatic-rebuild: 1
```

This has two major advantages:

1. Optimally sized partitions in the most recent will be the norm. This improves
   query performance throughout the board because of the reduced overall number
   of partitions.

2. Automatic rebuilding also helps with keeping partitions at the latest
   version. This makes it faster to adopt VAST versions that include breaking
   changes in the storage layout.

We also changed VAST to cut off underful partitions after 5 minutes rather than
1 hour by default (controlled through the option
`vast.active-partition-timeout`), reducing the risk of data loss in case of a
crash. Merging of undersized partitions ensures that this has no negative impact
on query performance.

## Optional Partition Indexes

You can now disable VAST's partition indexes per field to save disk space at the
cost of making queries affecting these fields slower.

In a drastically simplified model, VAST has three layers to its query
evaluation:

1. Send the query to the catalog, which is responsible for maintaining VAST's
   partitions, and ask it for a list of candidate partitions. The catalog
   maintains a sparse index per partition that we call the catalog index.

2. Send the query to all candidate partitions in parallel, which each contain a
   dense index for every field in the partition's schema that allow for
   identifying candidate events within the partition.

3. Send the query to all candidate partition's store backends if candidate
   events exist, evaluating the query against the candidate events to extract or
   count the resulting events.

We now added an option to disable partition indexes for selected fields, which
essentially causes VAST to skip step (2). This saves disk space and memory
usage, but makes most queries affecting these fields more compute-expensive to
run.

Here's how you can configure a partition index to be disabled:

```yaml
vast:
  index:
    rules:
        # Don't create partition indexes the suricata.http.http.url field.
      - targets:
          - suricata.http.http.url
        partition-index: false
        # Don't create partition indexes for fields of type addr.
      - targets:
          - :addr
        partition-index: false
```

## Improved Responsiveness Under High Load

Two small changes improve VAST's behavior under exceptionally high load.

First, the new `vast.connection-timeout` option allows for modifying the default
client to server connection timeout of 10 seconds. Previously, if a VAST server
was too busy to respond to a new client within 10 seconds, the client simply
exited with an unintelligable `!! request_timeout` error message. Here's how you
can set a custom timeout:

```yaml
vast:
  # The timeout for connecting to a VAST server. Set to 0 seconds to wait
  # indefinitely.
  connection-timeout: 10s
```

The option is additionally available under the environment variable
`VAST_CONNECTION_TIMEOUT` and the `--connection-timeout` command-line option.

Second, we improved the operability of VAST servers under high load from
automated low-priority queries. We noticed that when spawning thousands of
automated retro-match queries that compaction would stall and make little
visible progress, risking the disk running full or no longer being compliant
with GDPR-related policies enforced by compaction.

To ensure that compaction's internal and regular user-issued queries work as
expected even in this scenario, VAST now considers queries issued with
`--low-priority`,  with even less priority compared to regular queries (down
from 33.3% to 4%) and internal high-priority queries used for rebuilding and
compaction (down from 12.5% to 1%).
