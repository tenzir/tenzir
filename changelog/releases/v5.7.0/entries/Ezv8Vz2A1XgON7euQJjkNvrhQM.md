---
title: "`save_tcp` now reconnects on network outages"
type: feature
author: tobim
created: 2025-06-26T14:13:23Z
pr: 5230
---

The `save_tcp` (`from "tcp://..."`) operator now tries to reconnect in case of recoverable errors such as network outages and in case the remote end disconnects.

You can use the new options `retry_delay: duration` and `max_retry_count: int` to tune the behavior to your needs. The default values are set to 30 seconds and 10 times respectively.
