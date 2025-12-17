---
title: "Fixed handling of `time` in `to_amazon_security_lake`"
type: bugfix
author: IyeOnline
created: 2025-08-05T16:23:37Z
pr: 5409
---

Previously events with a `null` value for the OCSF `time` field would
incorrectly be written to some partition in the lake. In rare circumstances,
this could also cause a crash.

The operator now correctly skips events without a valid `time`.
