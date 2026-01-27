---
title: Stable memory usage for `from_http` server
type: bugfix
author: raxyte
pr: 5677
created: 2026-01-23T16:16:40.448298Z
---

The `from_http` server now has a stable memory usage when used with a slow
downstream, especially in situations where the client timeouts and retries
requests.
