---
title: "Fix possible garbage in status command output"
type: bugfix
authors: dominiklohmann
pr: 1872
---

The status command no longer occasionally contains garbage keys when the VAST
server is under high load.
