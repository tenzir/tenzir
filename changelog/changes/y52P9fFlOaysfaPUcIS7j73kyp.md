---
title: "Override lookup-table context entries for duplicate keys"
type: bugfix
authors: dominiklohmann
pr: 3808
---

Updating entries of a `lookup-table` context now overrides values with duplicate
keys instead of ignoring them.
