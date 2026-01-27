---
title: "Tweak the execution node behavior"
type: bugfix
author: dominiklohmann
created: 2023-08-24T10:50:15Z
pr: 3470
---

Pipeline operators that create output independent of their input now emit their
output instantly instead of waiting for receiving further input. This makes the
`shell` operator more reliable.

The `show <aspect>` operator wrongfully required unsafe pipelines to be allowed
for some aspects. This is now fixed.
