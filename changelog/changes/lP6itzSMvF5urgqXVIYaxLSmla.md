---
title: "Fix the schema name in `show contexts`"
type: bugfix
authors: dominiklohmann
pr: 4082
---

The schema name of events returned by `show contexts` sometimes did not match
the type of the context. This now works reliably.
