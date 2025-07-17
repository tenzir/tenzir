---
title: "Fix a logic error for retried requests in `/serve`"
type: bugfix
authors: dominiklohmann
pr: 4585
---

The `/serve` endpoint now gracefully handles retried requests with the same
continuation token, returning the same result for each request.
