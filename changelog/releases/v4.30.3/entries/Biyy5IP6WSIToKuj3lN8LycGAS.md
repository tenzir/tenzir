---
title: "Fix up `to_azure_log_analytics`"
type: bugfix
author: dominiklohmann
created: 2025-03-25T16:14:02Z
pr: 5077
---

We fixed a hang in `to_azure_log_analytics` for pipelines that never exhausted
their input.
