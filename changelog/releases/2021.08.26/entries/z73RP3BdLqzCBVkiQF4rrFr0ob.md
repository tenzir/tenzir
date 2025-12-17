---
title: "Interpret metrics paths relative to the db root"
type: bugfix
author: dominiklohmann
created: 2021-08-17T12:08:45Z
pr: 1848
---

The configuration options `vast.metrics.{file,uds}-sink.path` now correctly
specify paths relative to the database directory of VAST, rather than the
current working directory of the VAST server.
