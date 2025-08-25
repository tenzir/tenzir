---
title: "Dynamic `namespace` and retry logic for `to_google_secops`"
type: feature
authors: raxyte
pr: 5446
---

The `to_google_secops` operator now retries requests which fail with a `5XX` or
a `429` status code. Additionally, the `namespace` option of the operator now
supports all expressions that evaluate to a `string`.
