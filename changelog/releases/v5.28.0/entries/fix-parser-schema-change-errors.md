---
title: Parser bug fixes for schema changes
type: bugfix
authors:
  - IyeOnline
pr: 5805
created: 2026-03-06T00:00:00.000000Z
---

Fixed multiple issues that could cause errors or incorrect behavior when the
schema of parsed events changes between records. This is particularly important
when ingesting data from sources that may add, remove, or modify fields over time.

Schema mismatch warnings for repeated fields in JSON objects (which Tenzir
interprets as lists) now include an explanatory hint, making it clearer what's
happening when a field appears multiple times where a single value was expected.
