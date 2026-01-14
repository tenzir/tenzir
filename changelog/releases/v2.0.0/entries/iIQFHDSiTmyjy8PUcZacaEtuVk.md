---
title: "Fix race condition with exporter timeouts"
type: bugfix
author: lava
created: 2022-03-30T08:10:21Z
pr: 2167
---

Some queries could get stuck when an importer would time out during the meta
index lookup. This race condition no longer exists.
