---
title: Use event timestamps for compaction rules
type: feature
authors:
  - jachris
pr: 5629
created: 2025-12-23T07:20:47.211293Z
---

Compaction rules can now use event timestamps instead of import time when selecting data by age. Configure this using the new optional `field` key in the compaction configuration.

Previously, compaction always used the import time to determine which partitions to compact. Now you can specify any timestamp field from your events:

```yaml
tenzir:
  compaction:
    time:
      rules:
        - name: compact-old-logs
          after: 7d
          field: timestamp  # Use event timestamp instead of import time
          pipeline: |
            summarize count=count(), src_ip
```

When `field` is not specified, compaction continues to use import time for backward compatibility.
