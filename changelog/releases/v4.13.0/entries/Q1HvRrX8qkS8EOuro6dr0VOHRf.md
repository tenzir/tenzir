---
title: "Add `cron` operator"
type: feature
author: IyeOnline
created: 2024-05-09T14:13:41Z
pr: 4192
---

The `cron "<cron expression>"` operator modifier executes an operator
on a schedule.
For example, `cron "* */10 * * * MON-FRI" from https://example.org/api`
queries an endpoint on every 10th minute, Monday through Friday.
