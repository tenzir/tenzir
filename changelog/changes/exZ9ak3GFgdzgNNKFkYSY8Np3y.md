---
title: "Fix `vast.export.json.omit-nulls` for nested records"
type: bugfix
authors: dominiklohmann
pr: 2447
---

The JSON export with `--omit-nulls` now correctly handles nested records whose
first field is `null` instead of dropping them entirely.
