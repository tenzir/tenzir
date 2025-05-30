---
title: "Gracefully deal with JSON to data conversion errors"
type: bugfix
authors: tobim
pr: 1250
---

Values in JSON fields that can't be converted to the type that is specified in
the schema won't cause the containing event to be dropped any longer.
