---
title: The all-new Tenzir
type: breaking
authors:
  - aljazerzen
  - Avaq
  - gitryder
  - IyeOnline
  - jachris
  - lava
  - mavam
  - raxyte
  - tobim
  - zedoraps
created: 2026-11-12T17:11:34.000000Z
---

Meet the next generation of Tenzir: a rebuilt Tenzir Platform, a new deployment
model, an MCP server for your AI agents, and a built-in assistant that helps you
along the way.

Want to try it out, ask questions, or share feedback? Join us on Discord at
https://discord.tenzir.com/.

**New platform and interface.** The Tenzir Platform comes with a redesigned web
interface for building, running, and observing your pipelines. On the command
line, `tenzir` runs pipelines locally with `tenzir run` and manages your
deployments, configurations, and packages from one place.

**Better configuration management.** Each deployment runs one configuration
that holds its pipelines, packages, and secrets. Edit it in the web interface
or with the `tenzir` command, and always know what a deployment runs.

**Live configuration rollout.** The new deployment model applies configuration
changes to running deployments live. Change a pipeline, and your deployments
pick it up.

**MCP.** The platform speaks the Model Context Protocol, so AI agents can
inspect and manage your deployments and pipelines directly.

**Assistant.** A built-in assistant in the web interface helps you write TQL,
build pipelines, and operate your deployments.

A migration path from Tenzir Node to the new deployment model is planned, but
not available yet. Until then, we continue to publish maintenance releases for
Tenzir Node v6.
