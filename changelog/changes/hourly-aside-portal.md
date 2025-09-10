---
title: "Use UUIDv7 for file naming in `to_hive` operator"
type: change
authors: jachris
pr: 5464
---

The `to_hive` operator now uses UUIDv7 instead of consecutive numbers for file naming within partitions. This change provides guaranteed uniqueness across concurrent processes and natural time-based ordering of files, preventing filename conflicts when multiple processes write to the same partition simultaneously.

Example output paths changed from:
- `/partition/1.json`
- `/partition/2.json`
To:
- `/partition/01234567-89ab-cdef-0123-456789abcdef.json`
- `/partition/01234568-cd01-2345-6789-abcdef012345.json`

UUIDv7 combines the benefits of timestamp-based ordering with collision resistance, making it ideal for distributed data processing scenarios.
