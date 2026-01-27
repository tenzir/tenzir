---
title: "Make the run loop of exec nodes cheaper"
type: bugfix
author: dominiklohmann
created: 2025-05-19T07:33:06Z
pr: 5193
---

Fixed a bug in the `load_tcp` operator that would
cause it to require server certificates for incoming
connections.
