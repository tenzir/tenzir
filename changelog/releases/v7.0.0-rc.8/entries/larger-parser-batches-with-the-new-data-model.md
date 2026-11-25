---
title: Improved performance downstream of read operators
type: change
authors:
  - IyeOnline
created: 2026-11-19T00:00:00Z
---

We tuned the size of batches produced by parsers such as `read_json`, `read_csv`, and `read_syslog`. This improves the throughput of the operators that process events downstream of a parser.
