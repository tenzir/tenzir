---
draft: true
title: VAST v2.4
description: XXX
authors: dominiklohmann
date: 2022-11-24
tags: [release, frontend, feather, parquet, docker]
---

[VAST v2.4][github-vast-release] completes VAST's switch to open storage formats
by default, and includes an early peek at three upcoming features for VAST: A
web plugin with an integrated frontend user interface, Docker Compose
configuration files for getting started with VAST faster and showing how to
integrate VAST into your security operations center, and our new Python bindings
that will make writing integrations easier.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.4.0

<!--truncate-->

## Open Storage

- new default for storage is feather
- zstd compression for feather and parquet now configurable
- old segment store is read only now
- rebuild will rebuild to feather automatically in the background
- reference existing blog post series

## Docker Compose

- basic docker compose env that we plan to extend in the future
- outline what we plan to do with this; more in future blog posts

## Web Plugin

- basic rest endpoint + web ui that we plan to extend in the future
- outline what we plan to do with this; more in future blog posts

## Python Bindings

- state of the art: most of the security ecosystem interfaces to python
- new python bindings with good arrow integration make it easy to work with the
  data
- show small pandas example for rendering an arrow table

## Other Changes

- build scaffolding produces debian packages
- rebuild now also rebatches
- load all plugins by default now
- new metrics keys: ingest-total, catalog.num-events-total,
  catalog.num-partitions-total, index.memory-usage, catalog.memory-usage,
  filesystem.*
- metrics uds hang fix
