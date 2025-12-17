---
title: "Tune defaults and demo-node experience"
type: change
author: tobim
created: 2023-07-07T15:40:58Z
pr: 3320
---

We reduced the default `batch-timeout` from ten seconds to one second in to
improve the user experience of interactive pipelines with data aquisition.

We reduced the default `active-partition-timeout` from 5 minutes to 30 seconds
to reduce the time until data is persisted.
