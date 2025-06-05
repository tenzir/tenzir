---
title: "PRs 1257-1289"
type: change
authors: tobim
pr: 1257
---

VAST preserves nested JSON objects in events instead of formatting them in a
flattened form when exporting data with `vast export json`. The old behavior can
be enabled with `vast export json --flatten`.
