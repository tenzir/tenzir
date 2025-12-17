---
title: "Better defaults for `load_kafka`"
type: change
author: jachris
created: 2025-09-25T13:21:29Z
pr: 5485
---

The `load_kafka` operators previously used `offset="end"` as the default, which
meant that it always started from the end of the topic. This default was now
changed to `"stored"`, such that the previously commited offset is used instead.
