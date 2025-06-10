---
title: "Support parallel connections in `from tcp`"
type: bugfix
authors: dominiklohmann
pr: 4084
---

The `tcp` connector now supports accepting multiple connections in parallel when
used with the `from` operator, parsing data separately per connection.
