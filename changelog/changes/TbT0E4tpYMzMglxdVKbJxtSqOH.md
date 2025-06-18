---
title: "Fix data parser precedence"
type: bugfix
authors: jachris
pr: 4523
---

IPv6 addresses with a prefix that is a valid duration, for example `2dff::` with
the prefix `2d`, now correctly parse as an IP instead of a string.
