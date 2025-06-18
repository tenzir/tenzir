---
title: "Support aging out data based on a query"
type: feature
authors: dominiklohmann
pr: 929
---

VAST now supports aging out existing data. This feature currently only concerns
data in the archive. The options `system.aging-frequency` and
`system.aging-query` configure a query that runs on a regular schedule to
determine which events to delete. It is also possible to trigger an aging cycle
manually.
