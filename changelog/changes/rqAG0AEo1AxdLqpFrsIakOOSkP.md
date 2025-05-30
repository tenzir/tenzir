---
title: "Add an option to `export` both past and future events"
type: bugfix
authors: IyeOnline
pr: 4203
---

`export --live` no longer buffers the last batch of event that was imported, and
instead immediately returns all imported events.
