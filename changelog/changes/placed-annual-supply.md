---
title: "Fixed an assertion in parsers"
type: bugfix
authors: IyeOnline
pr: 5595
---

When parsing typed-data (e.g. integers in JSON), with a predefined schema that
expected a different type (e.g. a `time`), the parser would crash with an
assertion failure.

This has now been resolved and the field will simply be null instead with a
warning being emitted.
