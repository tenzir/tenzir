---
title: "Fail properly when transfer breaks"
type: bugfix
authors: mavam
pr: 3842
---

Failing transfers using `http(s)` and `ftp(s)` connectors now properly return an
error when the transfer broke. For example, `from http://does.not.exist` no
longer returns silently a success.
