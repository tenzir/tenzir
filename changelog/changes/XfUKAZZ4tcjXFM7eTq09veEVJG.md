---
title: "Refactor importer initialization"
type: bugfix
authors: tobim
pr: 647
---

In some cases it was possible that a source would connect to a node before it
was fully initialized, resulting in a hanging `vast import` process.
