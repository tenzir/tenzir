---
title: "`session_name` and `external_id` in `aws_iam` options"
type: bugfix
author: raxyte
created: 2025-09-22T13:28:06Z
pr: 5481
---

The `load_kafka`, `save_kafka` and `to_kafka` operators now accept configuring
`session_name` and `external_id` for `aws_iam` options.
