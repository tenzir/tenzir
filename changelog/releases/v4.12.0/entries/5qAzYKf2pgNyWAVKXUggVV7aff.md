---
title: "Add a `--timeout <duration>` option to `batch`"
type: feature
author: dominiklohmann
created: 2024-04-05T10:36:06Z
pr: 4095
---

The `batch` operator gained a new `--timeout <duration>` option that controls
the maixmum latency for withholding events for batching.
