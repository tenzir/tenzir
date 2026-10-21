---
title: Reading JSON split within UTF-8 characters
type: bugfix
authors:
  - mavam
created: 2026-10-13T17:54:45.204614Z
---

The `read_json` operator no longer fails with `UTF8_ERROR` when an input chunk ends within a multi-byte UTF-8 character. This could happen when reading valid JSON over HTTP, where chunk boundaries fall at arbitrary bytes, for example with `from_http "…" { read_json }`. Format detection with `read_auto` also no longer rejects such JSON input.
