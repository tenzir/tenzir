---
title: "PRs 2693-2923"
type: change
author: patszt
created: 2022-12-16T19:23:36Z
pr: 2693
---

Building VAST now requires CAF 0.18.7. VAST supports setting advanced options
for CAF directly in its configuration file under the `caf` section. If you were
using any of these, compare them against the bundled `vast.yaml.example` file to
see if you need to make any changes. The change has (mostly positive)
[performance and stability
implications](https://www.actor-framework.org/blog/2021-01/benchmarking-0.18/)
throughout VAST, especially in high-load scenarios.
