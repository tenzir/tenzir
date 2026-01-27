---
title: "Fixed `from_http` default port"
type: bugfix
author: jachris
created: 2025-08-01T21:15:56Z
pr: 5398
---

Using the `from_http` operator as a client without explicitly specifying a port
resulted in an error complaining that the port cannot be zero. This now works as
expected, meaning that the default port is derived correctly from the URL
scheme, i.e., 80 for HTTP and 443 for HTTPS.
