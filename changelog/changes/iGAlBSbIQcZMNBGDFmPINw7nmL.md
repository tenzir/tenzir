---
title: "Fix HTTP saver Content-Length computation"
type: bugfix
authors: mavam
pr: 4134
---

The `http` saver now correctly sets the `Content-Length` header value for HTTP
POST requests.
