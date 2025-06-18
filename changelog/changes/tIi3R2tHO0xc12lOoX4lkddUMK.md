---
title: "Use the timestamp type for inferred event timestamp fields in the Zeek reader"
type: bugfix
authors: tobim
pr: 2155
---

The `import zeek` command now correctly marks the event timestamp using the
`timestamp` type alias for all inferred schemas.
