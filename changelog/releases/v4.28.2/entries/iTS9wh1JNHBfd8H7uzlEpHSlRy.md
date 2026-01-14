---
title: "Fix crash in AMQP and add JSON `arrays_of_objects` option"
type: bugfix
author: IyeOnline
created: 2025-02-13T08:16:34Z
pr: 4994
---

We fixed a bug in `load_amqp` and `save_amqp` that prevented the node from
starting if they were used in a pipeline configured as code and failed to connect.
