---
title: "Add support for user-defined operator aliases"
type: change
author: jachris
created: 2023-04-12T17:15:27Z
pr: 3067
---

The `vast.operators` section in the configuration file supersedes the now
deprecated `vast.pipelines` section and more generally enables user-defined
operators. Defined operators now must use the new, textual format introduced
with VAST v3.0, and are available for use in all places where pipelines
are supported.
