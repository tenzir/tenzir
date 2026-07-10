---
title: Improved Platform Performance Support
type: feature
authors:
  - lava
prs:
  - 6373
created: 2026-07-10T13:00:14.913117Z
---

In order to improve the performance of the Tenzir Platform, we made a few
internal changes to the Tenzir Node. Notably the `/serve-multi` endpoint
received several improvements that reduce latency and overhead when driving the
frontend, making pipeline output fetching more responsive.

This does not requires any user action. We recommend that you do not manually
use the `serve` operator or endpoints.
