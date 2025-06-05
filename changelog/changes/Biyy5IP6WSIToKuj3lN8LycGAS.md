---
title: "Fix up `to_azure_log_analytics`"
type: bugfix
authors: dominiklohmann
pr: 5077
---

We fixed a hang in `to_azure_log_analytics` for pipelines that never exhausted
their input.
