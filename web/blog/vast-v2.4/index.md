---
draft: true
title: VAST v2.4
description: Open Storage
authors: dominiklohmann
date: 2022-11-24
tags: [release, frontend, feather, parquet, docker]
---

[VAST v2.4][github-vast-release] completes VAST's switch to open storage formats
by default, and includes an early peek at three upcoming features for VAST: A
web plugin with a REST API and an integrated frontend user interface, Docker
Compose configuration files for getting started with VAST faster and showing how
to integrate VAST into your security operations center, and our new Python
bindings that will make writing integrations easier and allow for using VAST
with your data science libraries like Pandas.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.4.0

<!--truncate-->

## Preventing Vendor Lock-in with Open Storage

VAST's Apache Feather (V2) and Apache Parquet storage backends are now
considered stable, and the default storage format is now Feather. This marks the
beginning of a new era for VAST: There is no more vendor lock-in for your data
in VAST.

Both as engineers and users of software we hate vendor lock-in by means of
holding data hostage. We want you to choose VAST because it's the best engine
for your security operations center, and not because someone decided in the past
to use it and now you're essentially stuck with it. As such, VAST is now not
just open-source, but also uses open standards for data storage.

VAST no longer supports _writing_ to its old proprietary storage format, but
will support _reading_ from it until the next major release. In the background,
VAST transparently rebuilds old partitions to take advantage of the new format
without any downtime. This may cause some additional load when starting VAST
first up after the update, but ensures that queries run as fast as possible once
all old partitions have been converted.

If you want to know more about Feather and Parquet, check out our in-depth blog
post series on them:
1. [Enabling Open Investigations][parquet-and-feather-1]
2. [Writing Security Telemetry][parquet-and-feather-2]
3. TBD (stay tuned!)

[parquet-and-feather-1]: /blog/parquet-and-feather-enabling-open-investigations/
[parquet-and-feather-2]: /blog/parquet-and-feather-writing-security-telemetry/

## What's Next?

VAST v2.4 contains a few new and experimental toys to play with. Here's an
overview of what they are, and how they all make it easier to integrate VAST
with other security tools.

### Docker Compose

A new set of [Docker Compose files][docker-compose] makes it easier than ever to
get started with VAST. This is not designed for high-performance deployments of
VAST, but rather to make it easier to try VAST out—all-batteries included,
because we want to use this to showcase and test the vast amount of integrations
possible in a modern security operations center.

Our vision for this is to show how VAST as a platform can power modern and
sustainable approaches to security.

[docker-compose]: /docs/setup/deploy/docker-compose

### REST API and Frontend User Interface

The experimental `web` plugin adds a [REST API][rest-api] to VAST, and also a
frontend user interface we [built in Svelte][frontend-code].

Both the API and the frontend are still considered unstable and subject to
change without notice. We plan to stabilize and version the API in the future.
Fundamentally, the API serves two purposes:
1. Make it easier to write integrations with VAST
2. Serve as a backend for VAST's bundled frontend

FIXME: Add screenshot of UI

[rest-api]: /api
[frontend-code]: https://github.com/tenzir/vast/tree/v2.4.0/plugins/web/ui

### Python Bindings

Tools found in a security operations center usually offer either a REST API or a
Python library for integrating with them. We want to make it as easy as possible
to integrate VAST with other tools, so we're working on making that as easy as
possible using VAST's Python bindings.

The new Python bindings allow data scientists to use industry-standard Python
libraries like Pandas to directly operate on data from VAST.

This is all enabled by our bet on open standards: VAST internally uses Apache
Arrow as its in-memory data representation. By writing a thin layer around
Apache Arrow's Python library we brought VAST's rich security-specific
extensions to Arrow's type system into Python, and then Arrow takes care of the
integration with libraries like Pandas for us.

::note Not yet on PyPI
VAST's new Python bindings are not yet on PyPI, as they are still heavily under
development. If you're too eager and cannot wait, go [check out the source
code][python-code].
:::

FIXME: Show example of how to use Pandas with an Arrow Table exported via VAST.

[python-code]: https://github.com/tenzir/vast/tree/v2.4.0/python

## Other Noteworthy Changes

A full list of changes to VAST since the last release is available in the
[changelog][changelog-2.4]. Here's a selection of changes that are particularly
noteworthy:
- VAST now loads all plugins by default. When asking new users for pitfalls they
  encountered, this ranked pretty high on the list of things we needed to
  change. To revert to the old behavior, set `vast.plugins: []` in your
  configuration file, or set `VAST_PLUGINS=` in your environment.
- The default endpoint changed from `localhost` to `127.0.0.1` to ensure a
  deterministic listening address.
- Exporting VAST's performance metrics via UDS no longer deadlocks VAST's
  metrics exporter when a listener is suspended.
- VAST's build process now natively supports building Debian packages. This
  makes upgrades for bare-metal deployments a breeze. As of this release, our
  CI/CD pipeline automatically attaches a Debian package in addition to the
  build archive to our releases.

[changelog-2.4]: /changelog#v240
