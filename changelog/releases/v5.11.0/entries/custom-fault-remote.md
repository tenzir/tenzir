---
title: "Performance improvements"
type: change
author: jachris
created: 2025-07-30T11:37:24Z
pr: 5382
---

Tenzir can now handle significantly more concurrent pipelines without becoming
unresponsive. These improvements make the system significantly more robust under
high load, with response times remaining stable even with thousands of
concurrent pipelines.
