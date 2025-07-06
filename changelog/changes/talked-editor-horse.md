---
title: "Fix `http` operator pagination"
type: bugfix
authors: mavam
pr: 5332
---

The `http` operator dropped all provided HTTP headers after the first request
when performing paginated requests. The operator now preserves the headers for
all requests.
