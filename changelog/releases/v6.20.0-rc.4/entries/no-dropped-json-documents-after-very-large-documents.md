---
title: No dropped JSON documents after very large documents
type: bugfix
authors:
  - mavam
created: 2026-10-13T18:38:35.299146Z
---

When `read_json` ran on the legacy executor and encountered a document larger than 10 MB, it silently dropped every document that followed in the same input chunk. It now keeps them.
