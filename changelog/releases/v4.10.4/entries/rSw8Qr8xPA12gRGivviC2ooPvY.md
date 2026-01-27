---
title: "Improve HTTP transfer abstraction"
type: bugfix
author: mavam
created: 2024-03-13T12:14:41Z
pr: 4031
---

The `http` saver now correctly sets the `Content-Length` header when issuing
HTTP requests.
