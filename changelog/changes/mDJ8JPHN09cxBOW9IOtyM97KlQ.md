---
title: "Handle large HTTP responses"
type: bugfix
authors: raxyte
pr: 5267
---

The HTTP client operators `from_http` and `http` now support response sizes of
upto 2GiB.
