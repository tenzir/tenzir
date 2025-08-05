---
title: "Assertion failures in `to_google_secops`"
type: bugfix
authors: raxyte
pr: 5411
---

The `to_google_secops` operator failed assertions when a batch of data was
missing `log_type` or if no input was received for longer than `batch_timeout`.
