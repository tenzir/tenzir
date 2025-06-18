---
title: "Improve HTTP transfer abstraction"
type: bugfix
authors: mavam
pr: 4031
---

The `http` saver now correctly sets the `Content-Length` header when issuing
HTTP requests.
