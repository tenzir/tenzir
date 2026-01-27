---
title: "Fix crash when using configured pipelines for the first time"
type: bugfix
author: dominiklohmann
created: 2024-03-11T20:07:07Z
pr: 4020
---

When upgrading from a previous version to Tenzir v4.10 and using configured
pipelines for the first time, the node sometimes crashed on startup. This no
longer happens.
