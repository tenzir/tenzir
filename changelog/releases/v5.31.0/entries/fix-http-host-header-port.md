---
title: Fix HTTP Host header missing port for non-standard ports
type: bugfix
created: 2026-03-31T00:00:00.000000Z
---

The `from_http` and `http` operators now include the port in the `Host` header
when the URL uses a non-standard port. Previously, the port was omitted, which
caused requests to fail with HTTP 403 when the server validates the `Host`
header against the full authority, such as for pre-signed URL signature
verification.
