---
title: "Implement AWS MSK IAM Authentication Mechanism for `{load,save}_kafka`"
type: feature
authors: raxyte
pr: 4944
---

The `load_kafka` and `save_kafka` operators can now authenticate with AWS MSK
using IAM via the new `aws_iam` options.
