---
title: VAST v1.1.2
description: VAST v1.1.2 - Compaction & Query Language Frontends
authors: lava
slug: 2022-03-29/vast-v1.1.2
tags: [release, compaction, query]
---

Dear community, we are happy to announce the release of [VAST
v1.1.2](https://github.com/tenzir/vast/releases/tag/v1.1.2), the latest release
on the VAST v1.1 series. This release contains a fix for a race condition that
could lead to VAST eventually becoming unresponsive to queries in large
deployments.

<!--truncate-->

Fixed a race condition that would cause queries to become stuck when an exporter
would time out during the meta index lookup.
[#2165](https://github.com/tenzir/vast/pull/2165)
