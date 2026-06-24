---
title: Graceful pipeline shutdown with data draining
type: feature
authors:
  - aljazerzen
created: 2026-05-21T19:28:37.839505Z
---

Stopping a pipeline or shutting down the node now drains in-flight data
before terminating, instead of discarding it. Source operators receive a
graceful stop signal and can finish outstanding work before the pipeline
shuts down.

A configurable grace period (`tenzir.shutdown-grace-period`, default
3 minutes) bounds how long the system waits. After the grace period
expires, remaining pipelines are force-killed.
