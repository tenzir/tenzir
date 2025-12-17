---
title: "Explicitly handle AMQP heartbeats in saver"
type: bugfix
author: satta
created: 2024-07-30T14:51:08Z
pr: 4428
---

Activating heartbeats via `-X`/`--set` on an `amqp` saver triggered socket errors
if the interval between sent messages was larger than the heartbeat interval.
This has been fixed by handling heartbeat communication correctly in such cases.
