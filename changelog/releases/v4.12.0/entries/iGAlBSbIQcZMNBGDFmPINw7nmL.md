---
title: "Fix HTTP saver Content-Length computation"
type: bugfix
author: mavam
created: 2024-04-23T20:37:05Z
pr: 4134
---

The `http` saver now correctly sets the `Content-Length` header value for HTTP
POST requests.
