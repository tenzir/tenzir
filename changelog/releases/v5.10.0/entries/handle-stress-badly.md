---
title: "Respecting error responses from Azure Log Analytics"
type: change
author: tobim
created: 2025-07-07T10:17:04Z
pr: 5314
---

The `to_azure_log_analytics` operator now emits an error when it receives any
response considering an internal error. Those normally indicate configuration
errors and the pipeline will now stop with an error instead of continuing to
send data that will not be received correctly.
