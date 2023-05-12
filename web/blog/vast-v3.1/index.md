---
title: VAST v3.1
authors: [tobim]
image: /img/blog/vast-v3.1.excalidraw.svg
date: 2023-05-12
tags: [release, pipelines, operators]
---

[VAST v3.1](https://github.com/tenzir/vast/releases/tag/v3.1.0) is out. This is
a small checkpointing release that brings a few new changes and fixes.

<!--truncate-->

## Pipelines Reloaded

The old pipeline execution engine is now gone and we updated VAST to use
the new engine everywhere. Most notably this applies to the `export` command,
the compaction engine, and the `query` REST interface.

For this release, we removed support for configuration level export and import
pipelines. This feature will make a return in the next major release.

We also removed the deprecated YAML-based pipeline syntax to fully concentrate
on the VAST Language.

## Operator Updates

We introduced several new operators:

- [`tail`](/docs/next/understand/operators/transformations/tail): limits the
  input to the last N events.
- [`unique`](/docs/next/understand/operators/transformations/unique): removes
  adjacent duplicates
- [`measure`](/docs/next/understand/operators/transformations/measure): replaces
  the input with incremental metrics describing the input.
- [`version`](/docs/next/understand/operators/sources/version): returns a single
  event displaying version information of VAST.
- [`from`](/docs/next/understand/operators/sources/from): produces events by
  combining a connector and a format.
- [`read`](/docs/next/understand/operators/sources/read): a short form of `from`
  that allows for omitting the connector.
- [`to`](/docs/next/understand/operators/sinks/to): consumes events by combining
  a connector and format.
- [`write`](/docs/next/understand/operators/sinks/write): a short form of `to`
  that allows for omitting the connector.

Additionally, the `put`, `replace`, and `extend` operators have been updated to
work with selectors and extractors. Check out the [growing list of
operators](/docs/next/understand/operators/).

## Operator Aliases

You can now define aliases for operators in the configuration file. Use it to
assign a short and reusable name for operators that would otherwise require
several arguments. For example:

```yaml
vast:
  operators:
    aggregate_flows: |
       summarize
         pkts_toserver=sum(flow.pkts_toserver),
         pkts_toclient=sum(flow.pkts_toclient),
         bytes_toserver=sum(flow.bytes_toserver),
         bytes_toclient=sum(flow.bytes_toclient),
         start=min(flow.start),
         end=max(flow.end)
       by
         timestamp,
         src_ip,
         dest_ip
       resolution
         10 mins
```

Now use it like a regular operator in a pipeline:

```
from file read suricata | aggregate_flows
```

## Notable Fixes

### Improved IPv6 Subnet Handling

The handling of subnets in the IPv6 space received multiple fixes:

- The expression `:ip !in ::ffff:0:0/96` now finds all events that
  contain IPs that cannot be represented as IPv4 addresses.
- Subnets with a prefix above 32 are now correctly formatted with
  an IPv6 network part, even if the address is representable as IPv4.

### A More Resilient Systemd Service

The systemd unit for VAST now automatically restarts the node in case the
process went down.
