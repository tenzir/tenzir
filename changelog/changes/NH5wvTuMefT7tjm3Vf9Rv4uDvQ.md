---
title: "Disable colors if `NO_COLOR` or not a terminal"
type: change
authors: jachris
pr: 3952
---

Color escape codes are no longer emitted if `NO_COLOR` is set to a non-empty
value, or when the output device is not a terminal.
