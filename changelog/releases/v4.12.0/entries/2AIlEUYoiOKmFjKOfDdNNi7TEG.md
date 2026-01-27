---
title: "Support parallel connections in `from tcp`"
type: bugfix
author: dominiklohmann
created: 2024-04-15T18:12:51Z
pr: 4084
---

The `tcp` connector now supports accepting multiple connections in parallel when
used with the `from` operator, parsing data separately per connection.
