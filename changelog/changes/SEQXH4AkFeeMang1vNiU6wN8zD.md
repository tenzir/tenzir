---
title: "Fix the `azure-log-analytics` operator (again)"
type: bugfix
authors: dominiklohmann
pr: 4578
---

The `azure-log-analytics` operator sometimes errored on startup complaining
about am unknown `window` option. This no longer occurs.
