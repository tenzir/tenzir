---
title: "Print the remote-version in the status command"
type: bugfix
authors: dominiklohmann
pr: 1652
---

VAST no longer erroneously skips the version mismatch detection between client
and server. The check now additionally compares running plugins.
