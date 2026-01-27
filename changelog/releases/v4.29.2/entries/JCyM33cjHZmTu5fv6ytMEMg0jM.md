---
title: "Ignore additional fields in package config"
type: bugfix
author: dominiklohmann
created: 2025-03-03T13:47:37Z
pr: 5031
---

Installing packages no longer fails when packages contain additional fields, and
instead warns about the unexpected fields.
