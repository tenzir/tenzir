---
title: "Use /etc as sysconfdir for install prefix /usr"
type: bugfix
author: dominiklohmann
created: 2021-08-20T10:43:30Z
pr: 1856
---

In order to align with the [GNU Coding
Standards](https://www.gnu.org/prep/standards/html_node/Directory-Variables.html),
the static binary (and other relocatable binaries) now uses `/etc` as
sysconfdir for installations to `/usr/bin/vast`.
