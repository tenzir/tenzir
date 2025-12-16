---
title: "Update default retention policies for metrics and diagnostics"
type: change
authors: lava
pr: 5594
---

Tenzir now applies default retention policies for internal metrics and diagnostics:

- **Metrics** (schema `tenzir.metrics.*`): Retained for 16 days by default
- **Diagnostics** (schema `tenzir.diagnostics.*`): Retained for 30 days by default

These defaults help manage storage usage while keeping sufficient history for troubleshooting. You can customize these settings:

```yaml
# tenzir.yaml
tenzir:
  retention:
    metrics: 16d      # Retention period for general metrics
    diagnostics: 30d  # Retention period for diagnostics
```

Set any retention period to `0` to disable automatic deletion for that category.
