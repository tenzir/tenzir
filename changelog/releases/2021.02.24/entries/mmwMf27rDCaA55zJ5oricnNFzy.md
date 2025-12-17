---
title: "Read user-supplied schema files from config dirs"
type: change
author: dominiklohmann
created: 2021-02-11T16:35:00Z
pr: 1372
---

User-supplied schema files are now picked up from `<SYSCONFDIR>/vast/schema` and
`<XDG_CONFIG_HOME>/vast/schema` instead of `<XDG_DATA_HOME>/vast/schema`.
