---
title: "Handle large HTTP responses"
type: bugfix
author: raxyte
created: 2025-06-06T16:33:28Z
pr: 5269
---

The HTTP client operators `from_http` and `http` now support response sizes upto
2 GiB.
