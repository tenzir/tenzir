---
draft: true
title: VAST v2.3
description: Automatic Rebuilds
authors: dominiklohmann
date: 2022-08-05
tags: [release, rebuild, performance]
---

We released [VAST v2.3][github-vast-release]!

FIXME: Write a short summary.

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

FIXME: Show the effect of this visually.

## Optional Dense Indexes

You can now disable VAST's dense indexes per field to save disk space at the
cost of making queries affecting these fields slower.

FIXME: Complete section.
