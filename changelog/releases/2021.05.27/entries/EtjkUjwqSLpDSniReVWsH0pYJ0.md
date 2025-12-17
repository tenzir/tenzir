---
title: "Print the remote-version in the status command"
type: bugfix
author: dominiklohmann
created: 2021-05-11T14:05:22Z
pr: 1652
---

VAST no longer erroneously skips the version mismatch detection between client
and server. The check now additionally compares running plugins.
