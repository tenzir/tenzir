---
title: Unresponsive pipeline API
type: bugfix
author: jachris
created: 2026-01-15T11:39:20.415787Z
pr: 5651
---

Previously, it was possible for the node to enter a state where the internal
pipeline API was no longer responding, thus rendering the platform unresponsive.
