---
title: Link header pagination for HTTP operators
type: feature
authors:
  - mavam
  - claude
created: 2026-02-11T10:59:34.22237Z
---

The `paginate` parameter for the `from_http` and `http` operators now supports link-based pagination via the `Link` HTTP header.

Previously, pagination was only available through a lambda function that extracted the next URL from response data. Now you can use `paginate="link"` to automatically follow pagination links specified in the response's `Link` header, following RFC 8288. This is useful for APIs that use HTTP header-based pagination instead of embedding next URLs in the response body.

The operator parses the `Link` header and follows the `rel=next` relation to automatically fetch the next page of results.

Example:

```
from_http "https://api.example.com/data", paginate="link"
```

If an invalid pagination mode is provided (neither a lambda nor `"link"`), the operator now reports a clear error message.
