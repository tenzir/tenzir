---
title: "Always format time values with microsecond precision"
type: change
authors: tobim
pr: 2380
---

VAST will from now on always format `time` and `timestamp` values with six
decimal places (microsecond precision). The old behavior used a precision that
depended on the actual value. This may require action for downstream tooling
like metrics collectors that expect nanosecond granularity.
