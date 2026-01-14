---
title: "Prepare repository for VAST v1.0.0-rc1"
type: change
author: dominiklohmann
created: 2022-01-20T10:15:41Z
pr: 2035
---

VAST no longer uses calendar-based versioning. Instead, it uses a semantic
versioning scheme. A new VERSIONING.md document installed alongside VAST
explores the semantics in-depth.

Plugins now have a separate version. The build scaffolding installs README.md
and CHANGELOG.md files in the plugin source tree root automatically.
