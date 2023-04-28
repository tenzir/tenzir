---
draft: true
title: VAST v3.1
description: Spring Codebase Cleaning
authors: [tobim]
image: /img/blog/building-blocks.excalidraw.svg
date: 2023-04-25
tags: [release, pipelines, operators]
---

<!--
TODO: Outstanding tasks for the blog post before we can publish
- Remove this comment
- Create an iamge for the blog post
--->

[VAST v3.1][github-vast-release] is out.

## Pipelines reloaded

The old pipeline execution engine has been removed and updated VAST to use
the new engine everywhere. Most notably in the `export` command, for the
compaction engine and in the `query` REST interface.

The transition process is not quite completed yet. For this release, we removed
support for configuration level export and import pipelines. This feature will
make a return in the next major release.

We also removed the deprecated YAML based pipeline syntax to fully concentrate
on the VAST Language.

Several new operators have been introduced, namely `tail`, `unique`, `from file`
The `put`, `replace`, and `extend` operators have been updated to work
with selectors and extractors. You are always invited to look at the docs for
our constantly growing [list of new operators][operators].

[operators]: /docs/understand/language/operators/

## Operator aliases

You can now define aliases for operators in the configuration file. You
can use it to assign a short and reusable name for operators that would
otherwise require several arguments. A nice illustrative example is:

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

You can then use it just like the builtin operators.

## Notable Fixes

### Improved IPv6 subnet handling

The handling of subnets in the IPv6 space received multiple fixes:
- The query `where :ip !in ::ffff:0:0/96` now finds all events that
  contain IPs that cannot be represented as IPv4 addresses.
- Subnets with a prefix above 32 are now correctly formatted with
  an IPv6 network part, even if the address is representable as IPv4.

### A more resilient systemd service

The systemd unit for VAST now automatically restarts the node in case the
process went down.
