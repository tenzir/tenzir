---
title: "Ignore types unrelated to the configuration in the summarize plugin"
type: bugfix
author: dominiklohmann
created: 2022-05-03T17:03:49Z
pr: 2258
---

Transform steps removing all nested fields from a record leaving only empty
nested records no longer cause VAST to crash.
