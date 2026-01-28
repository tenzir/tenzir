---
title: HTTP header values can contain colons
type: bugfix
authors:
  - lava
pr: 5693
created: 2026-01-28T15:08:15.430919Z
---

The curl HTTP header parsing now correctly handles header values containing colons. Previously, the parser expected header values to contain at most one colon character and would assert when encountering additional colons. This affected HTTP headers like `Authorization: Bearer abc: def` that legitimately contain colons in their values. The parser now uses proper split semantics to split only on the first colon, allowing any number of colons in the header value.
