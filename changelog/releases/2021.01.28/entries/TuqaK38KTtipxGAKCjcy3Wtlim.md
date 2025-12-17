---
title: "Gracefully deal with JSON to data conversion errors"
type: bugfix
author: tobim
created: 2020-12-21T18:52:50Z
pr: 1250
---

Values in JSON fields that can't be converted to the type that is specified in
the schema won't cause the containing event to be dropped any longer.
