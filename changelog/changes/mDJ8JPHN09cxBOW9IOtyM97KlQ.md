---
title: "Handle large HTTP responses"
type: bugfix
authors: raxyte
pr: 5269
---

The HTTP client operators `from_http` and `http` now support response sizes upto
2 GiB.
