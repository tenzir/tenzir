---
title: "Ignore additional fields in package config"
type: bugfix
authors: dominiklohmann
pr: 5031
---

Installing packages no longer fails when packages contain additional fields, and
instead warns about the unexpected fields.
