---
title: "Fix race condition with exporter timeouts"
type: bugfix
authors: lava
pr: 2167
---

Some queries could get stuck when an importer would time out during the meta
index lookup. This race condition no longer exists.
