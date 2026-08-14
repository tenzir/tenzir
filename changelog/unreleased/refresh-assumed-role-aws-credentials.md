---
title: Refresh assumed-role AWS credentials
type: bugfix
authors:
  - raxyte
created: 2026-08-14T00:00:00.000000Z
---

AWS-backed operators now refresh assumed-role sessions when `aws_iam` combines
an explicit access key and secret key with `assume_role`. Previously, these
pipelines failed when the initial STS session expired, typically after one hour.
