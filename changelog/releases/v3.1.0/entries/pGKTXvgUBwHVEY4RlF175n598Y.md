---
title: "Upgrade partition transformer to new pipelines"
type: feature
author: jachris
created: 2023-04-13T16:32:09Z
pr: 3064
---

User-defined operator aliases make pipelines easier to use by enabling users to
encapsulate a pipelinea into a new operator. To define a user-defined operator
alias, add an entry to the `vast.operators` section of your configuration.

Compaction now makes use of the new pipeline operators, and allows pipelines to
be defined inline instead in addition to the now deprecated `vast.pipelines`
configuration section.
