---
title: "Fix `http` operator pagination"
type: bugfix
author: mavam
created: 2025-07-07T07:41:41Z
pr: 5332
---

The `http` operator dropped all provided HTTP headers after the first request
when performing paginated requests. The operator now preserves the headers for
all requests.
