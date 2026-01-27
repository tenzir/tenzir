---
title: "Fix a logic error for retried requests in `/serve`"
type: bugfix
author: dominiklohmann
created: 2024-09-09T11:27:45Z
pr: 4585
---

The `/serve` endpoint now gracefully handles retried requests with the same
continuation token, returning the same result for each request.
