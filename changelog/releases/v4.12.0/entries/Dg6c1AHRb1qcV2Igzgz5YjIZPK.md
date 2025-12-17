---
title: "Support 0mq inproc sockets"
type: change
author: mavam
created: 2024-04-12T18:54:15Z
pr: 4117
---

The `0mq` connector no longer automatically monitors TCP sockets to wait until
at least one remote peer is present. Explicitly pass `--monitor` for this
behavior.
