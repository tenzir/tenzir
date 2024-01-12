---
title: Tenzir v4.8
authors: [dominiklohmann]
date: 2024-02-01
tags: [fluent-bit, performance]
draft: true
comments: true
---

[Tenzir v4.8](https://github.com/tenzir/tenzir/releases/tag/v4.8.0) .

<!--![Tenzir v4.8](tenzir-v4.8.excalidraw.svg)-->

<!-- truncate -->

## Theme A

TBD

## Theme B

TBD

## Fluent Bit Performance

The [`fluent-bit`](/operators/fluent-bit) source operator got a significant
performance boost as a byproduct of changing the Fluent Bit data exchange format
from JSON to MsgPack:

![Fluent Bit Performance](fluent-bit-speedup.svg)

Read the [dedicated blog post on this
issue](/blog/switching-fluentbit-from-json-to-msgpack).

Thanks to Christoph Lobmeyer and Yannik Meinhardt for reporting this issue! 🙏

## Improved Pipeline State Persistence

We've improved the state management of pipelines when nodes restart or crash.
Recall the state machine of a pipeline:

![Pipeline States](pipeline-states.excalidraw.svg)

The grey buttons on the state transition arrows correspond to actions you can
take.

Here's what changed on node restart and/or crash:

- Running pipelines remain in *Running* state. Previously, the node stopped all
  running pipelines when shutting down. The unexpected behavior was that a
  restart of a node didn't automatically resume previously running pipelines.
  This is now the case.
- Paused pipelines transition to the *Stopped* state. The difference between
  *Paused* and *Stopped* is that paused pipelines can be quickly without losing
  in-memory state. Stopping a pipeline fully evicts it. Since a node restart
  necessarily evicts the state of a pipeline, hence the transition from *Paused*
  to *Stopped*. Previously, paused pipelines were stopped.

## Here & There

If you're curious, [our changelog](/changelog#v480) has the full list of
changes.

Visit [app.tenzir.com](https://app.tenzir.com) to try the new
features and swing by [our Discord server](/discord) to get help and talk about
your use cases.
