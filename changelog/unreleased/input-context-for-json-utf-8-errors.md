---
title: Input context for JSON UTF-8 errors
type: bugfix
authors:
  - tobim
created: 2026-09-13T12:24:18.261223Z
---

JSON parsing errors caused by invalid UTF-8 now show the surrounding input with the invalid bytes escaped, including when reading HTTP responses with `from_http`.
