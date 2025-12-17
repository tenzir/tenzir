---
title: "Fix possible garbage in status command output"
type: bugfix
author: dominiklohmann
created: 2021-08-31T07:20:14Z
pr: 1872
---

The status command no longer occasionally contains garbage keys when the VAST
server is under high load.
