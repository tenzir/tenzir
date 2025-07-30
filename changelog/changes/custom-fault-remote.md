---
title: "Performance improvements"
type: change
authors: jachris
pr: 5382
---

Tenzir can now handle significantly more concurrent pipelines without becoming
unresponsive. These improvements make the system significantly more robust under
high load, with response times remaining stable even with thousands of
concurrent pipelines.
