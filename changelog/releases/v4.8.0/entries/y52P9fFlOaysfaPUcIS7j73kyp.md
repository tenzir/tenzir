---
title: "Override lookup-table context entries for duplicate keys"
type: bugfix
author: dominiklohmann
created: 2024-01-12T11:46:08Z
pr: 3808
---

Updating entries of a `lookup-table` context now overrides values with duplicate
keys instead of ignoring them.
