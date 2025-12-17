---
title: "Implement AWS MSK IAM Authentication Mechanism for `{load,save}_kafka`"
type: feature
author: raxyte
created: 2025-01-23T16:55:58Z
pr: 4944
---

The `load_kafka` and `save_kafka` operators can now authenticate with AWS MSK
using IAM via the new `aws_iam` options.
