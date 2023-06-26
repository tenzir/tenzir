---
title: Migrating from VAST to Tenzir
authors: dominiklohmann
date: 2023-06-26
tags: [tenzir, vast, community, project]
---

VAST is now Tenzir. This blog post describes what changed when [we renamed the
project](/blog/vast-to-tenzir).

![VAST to Tenzir](vast-to-tenzir.excalidraw.svg)

<!--truncate-->

## TL;DR

- Use `tenzir-node` instead of `vast start`.
- Use `tenzir` instead of `vast exec`.
- Use `tenzir-ctl` for all other commands.
- Move your configuration from `<prefix>/etc/vast/vast.yaml` to
  `<prefix>/etc/tenzir/tenzir.yaml`.
- Move your configuration from `$XDG_CONFIG_HOME/vast/vast.yaml` to
  `$XDG_CONFIG_HOME/tenzir/tenzir.yaml`.
- In your configuration, replace `vast:` with `tenzir:`.
- Prefix environment variables with `TENZIR_` instead of `VAST_`.

In addition to that, the following things have changed.

## Project

- The repository moved from `tenzir/vast` to `tenzir/tenzir`.
- Our Discord server is now the *Tenzir Community*. Join us at
  <https://discord.tenzir.com>!
- The documentation moved from [vast.io](https://vast.io) to
  [docs.tenzir.com](https://docs.tenzir.com).

## Usage

- We're making the split between starting a node and starting a pipeline more
  obvious:
  - `tenzir` executes pipelines (previously `vast exec`).
  - `tenzir-node` starts a node (previously `vast start`).
  - Some commands have not yet been ported over to pipelines, and are accessible
    under `tenzir-ctl`; this will be phased out over time without deprecation
    notices as commands are moving into pipeline operators.
  - The `vast` executable exists for drop-in backwards compatibility and is
    equivalent to running `tenzir-ctl`.
- Configuration moved to use `tenzir` over `vast` where possible.
- Packages are now called *Tenzir* instead of *VAST*.
- The default install prefix of packages moved from `/opt/vast` to `/opt/tenzir`.
- The Docker image now includes the proprietary plugins
- There exit separate Docker images `tenzir/tenzir` and `tenzir/tenzir-node` to
  match the new binaries `tenzir` and `tenzir-node`, respectively.
- The PyVAST package is deprecated and now called Tenzir. We will bring it back
  with the Tenzir v4.0 release.
- The interop with Apache Arrow uses `tenzir.` prefixes for the extension type
  names now. We support reading the old files transparently, but tools
  interfacing will need to adapt to the new names `tenzir.ip`, `tenzir.subnet`,
  and `tenzir.enumeration`.
