---
title: "Make the expression evaluator support heterogeneous results"
type: bugfix
author: jachris
created: 2024-12-14T18:22:50Z
pr: 4839
---

Metadata such as `@name` can now be set to a dynamically computed value that
does not have to be a constant. For example, if the field `event_name` should be
used as the event name, `@name = event_name` now correctly assigns the events
their name instead of using the first value.
