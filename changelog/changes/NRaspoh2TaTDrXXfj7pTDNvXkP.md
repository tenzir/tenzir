---
title: "Use /etc as sysconfdir for install prefix /usr"
type: bugfix
authors: dominiklohmann
pr: 1856
---

In order to align with the [GNU Coding
Standards](https://www.gnu.org/prep/standards/html_node/Directory-Variables.html),
the static binary (and other relocatable binaries) now uses `/etc` as
sysconfdir for installations to `/usr/bin/vast`.
