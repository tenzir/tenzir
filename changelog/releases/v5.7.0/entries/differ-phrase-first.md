---
title: "Kafka operators now automatically configure SASL mechanism for AWS IAM"
type: change
author: raxyte
created: 2025-07-01T15:31:01Z
pr: 5307
---

The `load_kafka` and `save_kafka` operators now automatically set
`sasl.mechanism` option to the expected `OAUTHBEARER` when using the `aws_iam`
option. If the mechanism has already been set to a different value, an error is
emitted.
