---
title: "Support 0mq inproc sockets"
type: change
authors: mavam
pr: 4117
---

The `0mq` connector no longer automatically monitors TCP sockets to wait until
at least one remote peer is present. Explicitly pass `--monitor` for this
behavior.
