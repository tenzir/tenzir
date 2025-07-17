---
title: "Kafka operators now automatically configure SASL mechanism for AWS IAM"
type: change
authors: raxyte
pr: 5307
---

The `load_kafka` and `save_kafka` operators now automatically set
`sasl.mechanism` option to the expected `OAUTHBEARER` when using the `aws_iam`
option. If the mechanism has already been set to a different value, an error is
emitted.
