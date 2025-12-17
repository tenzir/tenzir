---
title: "Remove long-deprecated code"
type: change
author: dominiklohmann
created: 2021-02-12T11:04:59Z
pr: 1374
---

The previously deprecated options `vast.spawn.importer.ids` and
`vast.schema-paths` no longer work. Furthermore, queries spread over multiple
arguments are now disallowed instead of triggering a deprecation warning.
