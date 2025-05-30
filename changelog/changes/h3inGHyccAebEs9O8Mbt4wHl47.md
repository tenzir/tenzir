---
title: "Detect and handle breaking changes in schemas"
type: bugfix
authors: dominiklohmann
pr: 1195
---

The type registry now detects and handles breaking changes in schemas, e.g.,
when a field type changes or a field is dropped from record.
