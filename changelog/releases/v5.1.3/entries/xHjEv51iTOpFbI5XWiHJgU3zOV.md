---
title: "Remove `pipeline::internal_parse()` from `to_azure_log_analytics`"
type: change
author: raxyte
created: 2025-05-05T11:59:31Z
pr: 5166
---

The `table` option of the `to_azure_log_analytics` has been renamed to `stream`
to better reflect the expected value. Additionally, a new option `batch_timeout`
has been added to configure the max duration to wait before finishing a batch.
